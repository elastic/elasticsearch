---
navigation_title: "{{es}} settings for ECH and ECE"
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-add-user-settings.html#ec-es-elasticsearch-settings
  - https://www.elastic.co/guide/en/cloud-enterprise/current/ece-add-user-settings.html#ece-change-user-settings-examples
applies_to:
  deployment:
    ess: 
    ece:
---

# Elasticsearch settings [add-user-settings]

Change how {{es}} runs by providing your own user settings. Elasticsearch Service appends these settings to each node’s `elasticsearch.yml` configuration file.

Elasticsearch Service automatically rejects `elasticsearch.yml` settings that could break your cluster. For a list of supported settings, check [Supported {{es}} settings](#TODO).

::::{tip}
Some settings that could break your cluster if set incorrectly are blocked, such as certain zen discovery and security settings. For examples of a few of the settings that are generally safe in cloud environments, check [Edit stack settings](docs-content://deploy-manage/deploy/cloud-enterprise/edit-stack-settings.md) for {{ece}} and  [Edit stack settings](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md) for the {{ech}} offering.
::::

::::{warning}
You can also update [dynamic cluster settings](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting) using {{es}}'s [update cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings). However, Elasticsearch Service doesn’t reject unsafe setting changes made using this API. Use with caution.
::::

## Add or edit user settings for {{ech}}:
```{applies_to}
  deployment:
    ess: 
```

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From your deployment menu, go to the **Edit** page.
4. In the **Elasticsearch** section, select **Manage user settings and extensions**.
5. Update the user settings.
6. Select **Save changes**.

::::{note}
In some cases, you may get a warning saying "User settings are different across Elasticsearch instances". To fix this issue, ensure that your user settings (including the comments sections and whitespaces) are identical across all Elasticsearch nodes (not only the data tiers, but also the Master, Machine Learning, and Coordinating nodes).
::::

## Add or edit user settings for {{ece}}:
```{applies_to}
  deployment:
    ece: 
```

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md).
2. On the **Deployments** page, select your deployment.

    Narrow the list by name, ID, or choose from several other filters. To further define the list, use a combination of filters.

3. From your deployment menu, go to the **Edit** page.
4. In the **Elasticsearch** section, select **Edit elasticsearch.yml**. For deployments with existing user settings, you may have to expand the **User setting overrides** caret for each node type instead.
5. Update the user settings.
6. Select **Save changes**.

    ::::{warning}
    If you encounter the **Edit elasticsearch.yml** carets, be sure to make your changes on all Elasticsearch node types.
    ::::

### Enable email notifications from Gmail [ece_enable_email_notifications_from_gmail]
```{applies_to}
  deployment:
    ece: 
```

You can configure email notifications to Gmail for a user that you specify. For details, refer to [Configuring email actions](docs-content://explore-analyze/alerts-cases/watcher/actions-email.md).

::::{warning}
Before you add the `xpack.notification.email*` setting in Elasticsearch user settings, make sure you add the account SMTP password to the keystore as a [secret value](docs-content://deploy-manage/security/secure-settings.md).
::::