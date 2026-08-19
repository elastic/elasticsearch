---
navigation_title: "AWS static credentials"
description: "Set up Amazon S3 static credentials for ES|QL Data Federation so Elasticsearch can read your private bucket with an access key and secret key."
applies_to:
  stack: experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Connect to Amazon S3 with static credentials for {{esql}} Data Federation

Static credentials let {{es}} read a private Amazon S3 data source using an AWS access key and secret key. You grant an IAM identity read-only access to your objects, generate a long-lived access key for it, and enter that key when you connect the data source.

Setup involves steps in both AWS and Elastic: create the IAM identity and access key in AWS, then enter the key when connecting the data source in Elastic.

You can use this page in two ways:

- Work through the following steps to understand each AWS resource and how the pieces fit together.
- Jump to the [complete AWS CLI example](#complete-aws-cli-example) to set it up hands-on and learn it by doing.

Refer to the [AWS IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) as the authoritative reference for the commands shown here.

## Before you begin

To follow this guide, you need:

- An Elastic project or deployment with {{esql}} Data Federation available.
- An AWS account with permissions to create IAM policies, users, and access keys.
- An S3 bucket containing the file or files you want to query.
- A role with the cluster manage privilege, or a `global.data_source` privilege, to create the data source. Refer to [Manage access](esql-data-federation-security.md).

## Connect to AWS with static credentials

Follow these steps to create and use dedicated credentials for use with Data Federation.

:::::::{stepper}

::::::{step} Create a read-only IAM policy
{{es}} reads your objects through an IAM identity, so first create an IAM policy that grants read-only access to only the objects you want to query in AWS. This policy is the part specific to this integration.

The following policy allows reading your objects with `s3:GetObject`, and listing the bucket with `s3:ListBucket` and `s3:GetBucketLocation` for prefix or glob queries:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [ "s3:GetObject" ],
      "Resource": [ "arn:aws:s3:::<bucket-name>/<path>/*" ] <1>
    },
    {
      "Effect": "Allow",
      "Action": [ "s3:ListBucket", "s3:GetBucketLocation" ],
      "Resource": [ "arn:aws:s3:::<bucket-name>" ] <2>
    }
  ]
}
```
1. Object-level actions apply to object ARNs. Narrow this to a prefix or a single file to grant the least access needed.
2. Bucket-level actions apply to the bucket ARN, not object ARNs. Include this statement only if you query by prefix or glob rather than a single fixed file.

:::{dropdown} Example: create the policy with the AWS CLI
Refer to the AWS IAM documentation for the authoritative steps and for console-based setup.

```shell
POLICY_ARN=$(aws iam create-policy \
  --policy-name parquet-sample-policy \
  --policy-document file://permissions-policy.json \
  --query 'Policy.Arn' --output text) <1>
```
1. A local file holding the preceding permissions policy. `create-policy` returns the policy ARN, which you attach to an identity in the next step.
:::
::::::

::::::{step} Attach the policy to an IAM identity
{{es}} authenticates to S3 as an IAM user. Attach the policy from the previous step to a dedicated user you create for {{es}}, or to an existing identity. Refer to the [AWS IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users.html) for guidance on managing IAM users.

:::{dropdown} Example: create a user and attach the policy with the AWS CLI
Create a user for {{es}} and attach the policy from the previous step:

```shell
# Create a dedicated IAM user for Elasticsearch
aws iam create-user --user-name esql-user

# Attach the read-only policy to the user
aws iam attach-user-policy \
  --user-name esql-user \
  --policy-arn "${POLICY_ARN}" <1>
```
1. The policy ARN returned in the previous step. Attaching the policy authorizes the user to read your data.
:::
::::::

::::::{step} Create an access key
Generate a long-lived access key for the user you authorized.

```shell
aws iam create-access-key --user-name esql-user
```

The response includes an **access key ID** and a **secret access key**. The secret is shown only once, so copy it now. These credentials do not expire on their own. They stay valid until you deactivate or delete them in AWS. This is the access key and secret key pair you enter in Elastic.
::::::

::::::{step} Connect the data source and create a dataset
Back in Elastic:

**Step 1.** Connect the S3 data source with the **Access and Secret Keys** method.

::::{tab-set}
:group: surface

:::{tab-item} UI
:sync: ui
In {{kib}}:

1. Go to **Data management** > **{{esql}} Data Federation**.
2. Click **Connect data source**.
3. Set **Data source type** to **Amazon S3**.
4. Under **Authentication**, select **Access and Secret Keys**.
5. Enter the **access key** and **secret key** from the previous step.

For the full field reference, refer to [Connect external data sources](esql-data-federation-sources.md).

:::{image} images/data-federation/access-and-secret-keys.png
:alt: The Authentication section of the Connect data source flyout with Access and Secret Keys selected, showing the access key and secret key fields
:width: 450px
:::
:::

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/prod_s3_static
{
  "type": "s3",
  "settings": {
    "region": "eu-north-1",
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>", <1>
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```
1. The access key ID and secret access key you created. {{es}} encrypts these before storing them. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_static" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "settings": {
    "region": "eu-north-1",
    "auth": "static_credentials",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}'
```
:::

::::

**Step 2.** [Create a dataset](esql-data-federation-datasets.md) that points at your files, for example `s3://amzn-s3-demo-bucket/some/sample.parquet` in **Parquet** format.

You can now query the remote data with {{esql}}.
::::::

:::::::

## Complete AWS CLI example

The preceding steps explain each AWS resource on its own. The following is an end-to-end AWS example of that setup, using sample values for one scenario. It is illustrative, not a script to run as-is: replace the example bucket, file, and names with your own before you run it. As with the individual steps, AWS is the authoritative reference for these commands.

:::{dropdown} Show the complete AWS CLI example
This example grants read access to a single Parquet file at `s3://amzn-s3-demo-bucket/some/sample.parquet` and creates a long-lived access key for a dedicated IAM user. Run the commands in order in [AWS CloudShell](https://docs.aws.amazon.com/cloudshell/latest/userguide/welcome.html) or any shell with the AWS CLI configured.

**Step 1.** Set the variables for your environment:

```shell
export BUCKET_NAME="amzn-s3-demo-bucket"
export FILE_NAME="some/sample.parquet"
export POLICY_NAME="parquet-sample-policy"
export IAM_USER="esql-user"
```

**Step 2.** Create a read-only IAM policy scoped to your objects, and capture the policy ARN it returns:

```shell
POLICY_ARN=$(aws iam create-policy \
  --policy-name "${POLICY_NAME}" \
  --policy-document "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [ "s3:GetObject" ],
      "Resource": [ "arn:aws:s3:::${BUCKET_NAME}/${FILE_NAME}" ]
    },
    {
      "Effect": "Allow",
      "Action": [ "s3:ListBucket", "s3:GetBucketLocation" ],
      "Resource": [ "arn:aws:s3:::${BUCKET_NAME}" ]
    }
  ]
}
EOF
)" \
  --query 'Policy.Arn' --output text)
```

**Step 3.** Create a dedicated IAM user for {{es}}:

```shell
aws iam create-user --user-name "${IAM_USER}"
```

**Step 4.** Attach the policy to the user:

```shell
aws iam attach-user-policy \
  --user-name "${IAM_USER}" \
  --policy-arn "${POLICY_ARN}"
```

**Step 5.** Generate a long-lived access key. Copy the access key ID and secret access key from the output, because the secret is shown only once:

```shell
aws iam create-access-key --user-name "${IAM_USER}"
```
:::

## Next steps

- [Query your data](esql-data-federation-querying.md) with `FROM`, including metadata columns and current limitations.
- [Create and manage datasets](esql-data-federation-datasets.md) to add more datasets over this data source, and configure file formats and settings.
- [Manage credentials and privileges](esql-data-federation-security.md) to control who can access your data sources and datasets.
