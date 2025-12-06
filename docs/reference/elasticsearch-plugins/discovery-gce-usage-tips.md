---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-tips.html
---

# GCE Tips [discovery-gce-usage-tips]

## Store project id locally [discovery-gce-usage-tips-projectid]

If you don’t want to repeat the project id each time, you can save it in the local gcloud config

```sh
gcloud config set project es-cloud
```


## Machine Permissions [discovery-gce-usage-tips-permissions]

If you have created a machine without the correct permissions, you will see `403 unauthorized` error messages. To change machine permission on an existing instance, first stop the instance then Edit. Scroll down to `Access Scopes` to change permission. The other way to alter these permissions is to delete the instance (NOT THE DISK). Then create another with the correct permissions.

Creating machines with gcloud
:   Ensure the following flags are set:

```text
--scopes=compute-rw
```


Creating with console (web)
:   When creating an instance using the web console, scroll down to **Identity and API access**.

Select a service account with the correct permissions or choose **Compute Engine default service account** and select **Allow default access** for **Access scopes**.


Creating with knife google
:   Set the service account scopes when creating the machine:

```sh
knife google server create www1 \
    -m n1-standard-1 \
    -I debian-8 \
    -Z us-central1-a \
    -i ~/.ssh/id_rsa \
    -x jdoe \
    --gce-service-account-scopes https://www.googleapis.com/auth/compute
```

Or, you may use the alias:

```sh
    --gce-service-account-scopes compute-rw
```
