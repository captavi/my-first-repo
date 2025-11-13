# deploy.py
import argparse
import io
import json
import os
import sys
import zipfile
import botocore
import boto3

def zip_bytes(file_path: str, arcname: str = "lambda_function.py") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(file_path, arcname=arcname)
    return buf.getvalue()

def upsert_lambda(lambda_client, function_name, role_arn, zip_bytes, region, env):
    # Try update first, otherwise create
    try:
        resp = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_bytes,
            Publish=True
        )
        # Update env too
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Handler="lambda_function.lambda_handler",
            Runtime="python3.12",
            Role=role_arn,
            Timeout=30,
            MemorySize=256,
            Environment={"Variables": env} if env else {}
        )
        return resp["FunctionArn"]
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        resp = lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=30,
            MemorySize=256,
            Publish=True,
            Environment={"Variables": env} if env else {}
        )
        return resp["FunctionArn"]

def add_invoke_permission(lambda_client, function_name, bucket_arn, statement_id="AllowS3Invoke"):
    # Idempotently add permission for S3 to invoke Lambda
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=bucket_arn
        )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            # already exists
            return
        raise

def configure_s3_notification(s3_client, bucket, lambda_arn, prefix=None, suffix=None):
    # Get existing notification config (to avoid clobbering)
    cfg = s3_client.get_bucket_notification_configuration(Bucket=bucket)

    filters = []
    if prefix:
        filters.append({"Name": "prefix", "Value": prefix})
    if suffix:
        filters.append({"Name": "suffix", "Value": suffix})

    lambda_config = {
        "LambdaFunctionArn": lambda_arn,
        "Events": ["s3:ObjectCreated:*"],
    }
    if filters:
        lambda_config["Filter"] = {"Key": {"FilterRules": filters}}

    # Merge with any existing Lambda configs
    existing = cfg.get("LambdaFunctionConfigurations", [])
    # Remove any that target the same arn to keep one latest version
    existing = [c for c in existing if c.get("LambdaFunctionArn") != lambda_arn]
    existing.append(lambda_config)

    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"LambdaFunctionConfigurations": existing}
    )

def main():
    parser = argparse.ArgumentParser(description="Deploy Lambda and attach S3 create-object trigger.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--function", required=True, help="Lambda function name")
    parser.add_argument("--role-arn", required=True, help="IAM role ARN for Lambda execution")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--prefix", default=None, help="Optional S3 key prefix filter")
    parser.add_argument("--suffix", default=None, help="Optional S3 key suffix filter, e.g. .csv")
    parser.add_argument("--sns-topic-arn", default=None)
    parser.add_argument("--slack-webhook-url", default=None)
    parser.add_argument("--file", default="lambda_function.py", help="Path to lambda file")
    args = parser.parse_args()

    session = boto3.Session(region_name=args.region)
    s3 = session.client("s3")
    lam = session.client("lambda")

    if not os.path.exists(args.file):
        print(f"Missing {args.file}", file=sys.stderr)
        sys.exit(1)

    code_zip = zip_bytes(args.file)

    env = {}
    if args.sns_topic_arn:
        env["SNS_TOPIC_ARN"] = args.sns_topic_arn
    if args.slack_webhook_url:
        env["SLACK_WEBHOOK_URL"] = args.slack_webhook_url

    lambda_arn = upsert_lambda(lam, args.function, args.role_arn, code_zip, args.region, env)

    add_invoke_permission(lam, args.function, f"arn:aws:s3:::{args.bucket}")

    configure_s3_notification(s3, args.bucket, lambda_arn, args.prefix, args.suffix)

    print("Done.")
    print("Lambda ARN:", lambda_arn)
    print(f"S3 bucket '{args.bucket}' now triggers '{args.function}' on ObjectCreated events.")

if __name__ == "__main__":
    main()
