---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud-enterprise/current/ece-add-plugins.html
---

# Plugin management (Cloud Enterprise) [ece-add-plugins]

Plugins extend the core functionality of Elasticsearch. Elastic Cloud Enterprise makes it easy to add plugins to your deployment by providing a number of plugins that work with your version of Elasticsearch. One advantage of these plugins is that you generally don’t have to worry about upgrading plugins when upgrading to a new Elasticsearch version, unless there are breaking changes. The plugins simply are upgraded along with the rest of your deployment.

Adding plugins to a deployment is as simple as selecting it from the list of available plugins, but different versions of Elasticsearch support different plugins. Plugins are available for different purposes, such as:

* National language support, phonetic analysis, and extended unicode support
* Ingesting attachments in common formats and ingesting information about the geographic location of IP addresses
* Adding new field datatypes to Elasticsearch

Additional plugins might be available. If a plugin is listed for your version of Elasticsearch, it can be used.

To add plugins when creating a new deployment:

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md) and select **Create deployment**.
2. Make your initial deployment selections, then select **Customize Deployment**.
3. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
4. Select the plugins you want.
5. Select **Create deployment**.

The deployment spins up with the plugins installed.

To add plugins to an existing deployment:

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md).
2. On the **Deployments** page, select your deployment.

    Narrow the list by name, ID, or choose from several other filters. To further define the list, use a combination of filters.

3. From your deployment menu, go to the **Edit** page.
4. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
5. Select the plugins that you want.
6. Select **Save changes**.

There is no downtime when adding plugins to highly available deployments. The deployment is updated with new nodes that have the plugins installed.

