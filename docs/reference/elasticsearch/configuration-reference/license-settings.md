---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/license-settings.html
applies_to:
  deployment:
    self:
---

# License settings [license-settings]

You can configure this licensing setting in the `elasticsearch.yml` file. For more information, see [License management](docs-content://deploy-manage/license/manage-your-license-in-self-managed-cluster.md).

`xpack.license.self_generated.type`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Set to `basic` (default) to enable basic {{xpack}} features.<br>

    If set to `trial`, the self-generated license gives access only to all the features of a x-pack for 30 days. You can later downgrade the cluster to a basic license if needed.


