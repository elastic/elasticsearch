---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
applies_to:
  stack: ga
  serverless: unavailable
---

# Plugin management

A plugin is installed on each node and loaded when that node starts. Both the plugins available to you and the way you install them depend on where {{es}} runs. Hosted deployments manage plugins for you through a console or API, while self-managed clusters use a command-line tool or a configuration file.

[Plugins and custom configuration files](docs-content://deploy-manage/plugins-and-custom-configuration-files.md) covers plugins alongside the other way to extend {{es}}: custom configuration files that {{es}} reads at runtime, such as synonym dictionaries, SAML metadata, and GeoIP databases. Refer to the page for your deployment type:

* [{{ech}}](docs-content://deploy-manage/plugins-and-custom-configuration-files/elastic-cloud/add-plugins-extensions.md): enable a plugin provided with your {{es}} version, upload a custom plugin or bundle, or manage either through the extensions API.
* [{{ece}}](docs-content://deploy-manage/plugins-and-custom-configuration-files/cloud-enterprise/add-plugins.md): enable a provided plugin, reference a custom bundle from an HTTP or HTTPS URL, or add a {{kib}} plugin through a custom image.
* [{{eck}}](docs-content://deploy-manage/plugins-and-custom-configuration-files/cloud-on-k8s/manage-plugins.md): build a custom container image or run an init container, and mount configuration files from a ConfigMap or Secret.
* [Self-managed](docs-content://deploy-manage/plugins-and-custom-configuration-files/self-managed/manage-plugins.md): declare the plugins you want in a [configuration file](manage-plugins-using-configuration-file.md) with the official Docker image, or run the [`elasticsearch-plugin`](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md) command-line tool for package and archive installs.

::::{admonition} {{serverless-full}} projects
{{serverless-full}} projects do not support installing plugins or uploading custom plugins and bundles. {{serverless-short}} includes [core analysis plugins](./analysis-plugins.md#_core_analysis_plugins) by default. To manage synonyms, use the [synonyms API]({{es-serverless-apis}}group/endpoint-synonyms).
::::
