---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
applies_to:
  stack: ga
  serverless: unavailable
---

# Plugin management

A plugin is installed on each node and loaded when that node starts. Both the plugins available to you and the way you install them depend on where {{es}} runs. Hosted deployments manage plugins for you through a console or API, while self-managed clusters use a command-line tool or a configuration file.

<!--
TEMPORARY LINKS. The docs-content "Plugins and configuration files" section does not exist
on docs-content main yet, so docs-content:// crosslinks fail the build. Every link below
that starts with https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/ is a
stand-in for the equivalent docs-content:// path.

When https://github.com/elastic/docs-content/pull/7959 merges, swap the prefix and add the
.md extension back, for example:
  https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files
  becomes docs-content://deploy-manage/plugins-and-configuration-files.md
-->

[Plugins and configuration files](https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files) covers both plugins and the configuration files that support them, such as synonym dictionaries, SAML metadata, and GeoIP databases. Refer to the page for your deployment type:

* [{{ech}}](https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files/elastic-cloud/add-plugins-extensions): enable a plugin provided with your {{es}} version, upload a custom plugin or bundle, or manage either through the extensions API.
* [{{ece}}](https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files/cloud-enterprise/add-plugins): enable a provided plugin, reference a custom bundle from an HTTP or HTTPS URL, or add a {{kib}} plugin through a custom image.
* [{{eck}}](https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files/cloud-on-k8s/manage-plugins): build a custom container image or run an init container, and mount configuration files from a ConfigMap or Secret.
* [Self-managed](https://docs-v3-preview.elastic.dev/elastic/docs-content/pull/7959/deploy-manage/plugins-and-configuration-files/self-managed/manage-plugins): declare the plugins you want in a [configuration file](manage-plugins-using-configuration-file.md) with the official Docker image, or run the [`elasticsearch-plugin`](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md) command-line tool for package and archive installs.

::::{admonition} {{serverless-full}} projects
{{serverless-full}} projects do not support installing plugins or uploading custom plugins and bundles. {{serverless-short}} includes [core analysis plugins](./analysis-plugins.md#_core_analysis_plugins) by default. To manage synonyms, use the [synonyms API]({{es-serverless-apis}}group/endpoint-synonyms).
::::
