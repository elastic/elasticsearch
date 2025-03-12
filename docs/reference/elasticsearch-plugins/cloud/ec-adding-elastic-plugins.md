---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-adding-elastic-plugins.html
---

# Add plugins provided with Elasticsearch Service [ec-adding-elastic-plugins]

You can use a variety of official plugins that are compatible with your version of {{es}}. When you upgrade to a new {{es}} version, these plugins are simply upgraded with the rest of your deployment.

## Before you begin [ec_before_you_begin_6]

Some restrictions apply when adding plugins. To learn more, check [Restrictions for {{es}} and {{kib}} plugins](cloud://release-notes/cloud-hosted/known-issues.md#ec-restrictions-plugins).

Only Gold, Platinum, Enterprise and Private subscriptions, running version 2.4.6 or later, have access to uploading custom plugins. All subscription levels, including Standard, can upload scripts and dictionaries.

To enable a plugin for a deployment:

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From the **Actions** dropdown, select **Edit deployment**.
4. Select **Manage user settings and extensions**.
5. Select the **Extensions** tab.
6. Select the plugins that you want to enable.
7. Select **Back**.
8. Select **Save**. The {{es}} cluster is then updated with new nodes that have the plugin installed.


