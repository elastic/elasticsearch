---
navigation_title: "Create index from source"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index-from-source.html
  # That link will 404 until 8.18 is current
  # (see https://www.elastic.co/guide/en/elasticsearch/reference/8.18/indices-create-index-from-source.html)
applies_to:
  stack: all
---

# Create index from source API [indices-create-index-from-source]


::::{admonition} New API reference
For the most up-to-date API details, refer to [Index APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-indices).

::::


## {{api-request-title}} [indices-create-index-from-source-api-request]

`PUT /_create_from/<source>/<dest>`

`POST/_create_from/<source>/<dest>`


## {{api-prereq-title}} [indices-create-index-from-source-api-prereqs]

* If the {{es}} {{security-features}} are enabled, you must have the `manage` [index privilege](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/elasticsearch-privileges.md#privileges-list-indices) for the index.


## {{api-description-title}} [indices-create-index-from-source-api-desc]

This api allows you to add a new index to an {{es}} cluster, using an existing source index as a basis for the new index. The settings and mappings from the source index will copied over to the destination index.  You can also provide override settings and mappings which will be combined with the source settings and mappings when creating the destination index.


## {{api-path-parms-title}} [indices-create-index-from-source-api-path-params]

`<source>`
:   (Required, string) Name of the existing source index which will be used as a basis.

`<dest>`
:   (Required, string) Name of the destination index which will be created.


## {{api-request-body-title}} [indices-create-index-from-source-api-request-body]

`settings_override`
:   (Optional, index setting object) [Index settings](/reference/elasticsearch/index-settings/index.md) which override the source settings.

`mappings_override`
:   (Optional, [mapping object](docs-content://manage-data/data-store/mapping.md)) Mappings which override the source mappings.

`remove_index_blocks`
:   (Optional, boolean) Filter out any index blocks from the source index when creating the destination index. Defaults to `true`.


## {{api-examples-title}} [indices-create-index-from-source-api-example]

Start by creating a source index that we’ll copy using this API.

```console
PUT /my-index
{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "blocks.write": true
    }
  },
  "mappings": {
    "properties": {
        "field1": { "type": "text" }
    }
  }
}
```

Now we create a destination index from the source index. This new index will have the same mappings and settings as the source index.

```console
POST _create_from/my-index/my-new-index
```

Alternatively, we could override some of the source’s settings and mappings. This will use the source settings and mappings as a basis and combine these with the overrides to create the destination settings and mappings.

```console
POST _create_from/my-index/my-new-index
{
  "settings_override": {
    "index": {
      "number_of_shards": 5
    }
  },
  "mappings_override": {
    "properties": {
        "field2": { "type": "boolean" }
    }
  }
}
```

Since the destination index is empty, we very likely will want to write into the index after creation. This would not be possible if the source index contains an [index write block](/reference/elasticsearch/index-settings/index-block.md) which is copied over to the destination index. One way to handle this is to remove the index write block using a settings override. For example, the following settings override removes all index blocks.

```console
POST _create_from/my-index/my-new-index
{
  "settings_override": {
    "index": {
      "blocks.write": null,
      "blocks.read": null,
      "blocks.read_only": null,
      "blocks.read_only_allow_delete": null,
      "blocks.metadata": null
    }
  }
}
```

Since this is a common scenario, index blocks are actually removed by default. This is controlled with the parameter `remove_index_blocks`, which defaults to `true`. If we want the destination index to contains the index blocks from the source index, we can do the following:

```console
POST _create_from/my-index/my-new-index
{
  "remove_index_blocks": false
}
```


