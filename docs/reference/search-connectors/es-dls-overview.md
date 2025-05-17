---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-dls-overview.html
---

# How DLS works [es-dls-overview]

Document level security (DLS) enables you to control access to content at the document level. Access to each document in an index can be managed independently, based on the identities (such as usernames, emails, groups etc.) that are allowed to view it.

This feature works with the help of special access control documents that are indexed by a connector into a hidden Elasticsearch index, associated with the standard content index. If your content documents have access control fields that match the criteria defined in your access control documents, Elasticsearch will apply DLS to the documents synced by the connector.


## Core concepts [es-dls-overview-core-concepts]

At a very high level, there are two essential components that enable document level security with connectors:

* **Access control documents**: These documents define the access control policy for documents from your third party source. They live in a hidden index named with the following pattern: `.search-acl-filter-<INDEX-NAME>`. See [access control documents](#es-dls-overview-access-control-documents) for more details and an example.
* **Content documents with access control fields**: The documents that contain the synced content from your third party source must have **access control fields** that match the criteria defined in your access control documents. These documents live in an index named with the following pattern: `search-<INDEX-NAME>`.

    * If a content document does not have access control fields, there will be no restrictions on who can view it.
    * If the access control field is present but *empty*, no identities will have access and the document will be effectively invisible.

        See [content documents](#es-dls-overview-content-documents) for more details.



## Enabling DLS [es-dls-overview-procedure]

To enable DLS, you need to perform the following steps:

1. First **enable DLS** for your connector as part of the connector configuration.
2. Run an **Access control** sync.
3. This creates a hidden access control index prefixed with `.search-acl-filter-`. For example, if you named your connector index `search-sharepoint`, the access control index would be named `.search-acl-filter-search-sharepoint`.
4. The [access control documents](#es-dls-overview-access-control-documents) on the hidden index define which identities are allowed to view documents with access control fields.
5. The access control document uses a search template to define how to filter search results based on identities.
6. Schedule recurring **Access control** syncs to update the access control documents in the hidden index.

Note the following details about content documents and syncs:

1. Remember that for DLS to work, your **content documents** must have access control fields that match the criteria defined in your access control documents. [Content documents](#es-dls-overview-content-documents) contain the actual content your users will search for. If a content document does not have access control fields, there will be no restrictions on who can view it.
2. When a user searches for content, the access control documents determine which content the user is allowed to view.
3. At *search* time documents without the `_allow_access_control` field or with allowed values in `_allow_access_control.enum` will be returned in the search results. The logic for determining whether a document has access control enabled is based on the presence or values of the `_allow_access_control*` fields.
4. Run **Content** syncs to sync your third party data source to Elasticsearch. A specific field (or fields) within these documents correlates with the query parameters in the access control documents enabling document-level security (DLS).

::::{note}
You must enable DLS for your connector *before* running the first content sync. If you have already run a content sync, you’ll need to delete all documents on the index, enable DLS, and run a new content sync.

::::



## DLS at index time [es-dls-overview-index]


### Access control documents [es-dls-overview-access-control-documents]

These documents define the access control policy for the data indexed into Elasticsearch. An example of an access control document is as follows:

```js
{
  "_id": "example.user@example.com",
  "identity": {
      "username": "example username",
      "email": "example.user@example.com"
   },
   "query": {
        "template": {
            "params": {
                "access_control": [
                    "example.user@example.com",
                    "example group",
                    "example username"]
            }
        },
        "source": "..."
    }
}
```
% NOTCONSOLE

In this example, the identity object specifies the identity of the user that this document pertains to. The `query` object then uses a template to list the parameters that form the access control policy for this identity. It also contains the query `source`, which will specify a query to fetch all content documents the identity has access to. The `_id` could be, for example, the email address or the username of a user. The exact content and structure of `identity` depends on the corresponding implementation.


### Content documents [es-dls-overview-content-documents]

Content documents contain the actual data from your 3rd party source. A specific field (or fields) within these documents correlates with the query parameters in the access control documents enabling document-level security (DLS). Please note, the field names used to implement DLS may vary across different connectors. In the following example we’ll use the field `_allow_access_control` for specifying the access control for a user identity.

```js
{
  "_id": "some-unique-id",
  "key-1": "value-1",
  "key-2": "value-2",
  "key-3": "value-3",
  "_allow_access_control": [
    "example.user@example.com",
    "example group",
    "example username"
  ]
}
```
% NOTCONSOLE


### Access control sync vs content sync [es-dls-overview-sync-type-comparison]

The ingestion of documents into an Elasticsearch index is known as a sync. DLS is managed using two types of syncs:

* **Content sync**: Ingests content into an index that starts with `search-`.
* **Access control sync**: Separate, additional sync which ingests access control documents into index that starts with `.search-acl-filter-`.

During a sync, the connector ingests the documents into the relevant index based on their type (content or access control). The access control documents determine the access control policy for the content documents.

By leveraging DLS, you can ensure that your Elasticsearch data is securely accessible to the right users or groups, based on the permissions defined in the access control documents.


## DLS at search time [es-dls-overview-search-time]


### When is an identity allowed to see a content document [es-dls-overview-search-time-identity-allowed]

A user can view a document if at least one access control element in their access control document matches an item within the document’s `_allow_access_control` field.


#### Example [es-dls-overview-search-time-example]

This section illustrates when a user has access to certain documents depending on the access control.

One access control document:

```js
{
  "_id": "example.user@example.com",
  "identity": {
      "username": "example username",
      "email": "example.user@example.com"
   },
   "query": {
        "template": {
            "params": {
                "access_control": [
                    "example.user@example.com",
                    "example group",
                    "example username"]
            }
        },
        "source": "..."
    }
}
```
% NOTCONSOLE

Let’s see which of the following example documents these permissions can access, and why.

```js
{
  "_id": "some-unique-id-1",
  "_allow_access_control": [
    "example.user@example.com",
    "example group",
    "example username"
  ]
}
```
% NOTCONSOLE

The user `example username` will have access to this document as he’s part of the corresponding group and his username and email address are also explicitly part of `_allow_access_control`.

```js
{
  "_id": "some-unique-id-2",
  "_allow_access_control": [
    "example group"
  ]
}
```
% NOTCONSOLE

The user `example username` will also have access to this document as they are part of the `example group`.

```js
{
  "_id": "some-unique-id-3",
  "_allow_access_control": [
    "another.user@example.com"
  ]
}
```
% NOTCONSOLE

The user `example username` won’t have access to this document because their email does not match `another.user@example.com`.

```js
{
  "_id": "some-unique-id-4",
  "_allow_access_control": []
}
```
% NOTCONSOLE

No one will have access to this document as the `_allow_access_control` field is empty.


### Querying multiple indices [es-dls-overview-multiple-connectors]

This section illustrates how to define an Elasticsearch API key that has restricted read access to multiple indices that have DLS enabled.

A user might have multiple identities that define which documents they are allowed to read. We can define an Elasticsearch API key with a role descriptor for each index the user has access to.


#### Example [es-dls-overview-multiple-connectors-example]

Let’s assume we want to create an API key that combines the following user identities:

```js
GET .search-acl-filter-source1
{
  "_id": "example.user@example.com",
  "identity": {
      "username": "example username",
      "email": "example.user@example.com"
   },
   "query": {
        "template": {
            "params": {
                "access_control": [
                    "example.user@example.com",
                    "source1-user-group"]
            }
        },
        "source": "..."
    }
}
```
% NOTCONSOLE

```js
GET .search-acl-filter-source2
{
  "_id": "example.user@example.com",
  "identity": {
      "username": "example username",
      "email": "example.user@example.com"
   },
   "query": {
        "template": {
            "params": {
                "access_control": [
                    "example.user@example.com",
                    "source2-user-group"]
            }
        },
        "source": "..."
    }
}
```
% NOTCONSOLE

`.search-acl-filter-source1` and `.search-acl-filter-source2` define the access control identities for `source1` and `source2`.

You can create an Elasticsearch API key using an API call like this:

```console
POST /_security/api_key
{
  "name": "my-api-key",
  "role_descriptors": {
    "role-source1": {
      "indices": [
        {
          "names": ["source1"],
          "privileges": ["read"],
          "query": {
            "template": {
                "params": {
                    "access_control": [
                        "example.user@example.com",
                        "source1-user-group"]
                }
            },
            "source": "..."
          }
        }
      ]
    },
    "role-source2": {
      "indices": [
        {
          "names": ["source2"],
          "privileges": ["read"],
          "query": {
            "template": {
                "params": {
                    "access_control": [
                        "example.user@example.com",
                        "source2-user-group"]
                }
            },
            "source": "..."
          }
        }
      ]
    }
  }
}
```
% TEST[skip:TODO]


#### Workflow guidance [es-dls-overview-multiple-connectors-workflow-guidance]

We recommend relying on the connector access control sync to automate and keep documents in sync with changes to the original content source’s user permissions.

Consider setting an `expiration` time when creating an Elasticsearch API key. When `expiration` is not set, the Elasticsearch API will never expire.

The API key can be invalidated using the [Invalidate API Key API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-api-key). Additionally, if the user’s permission changes, you’ll need to update or recreate the Elasticsearch API key.


### Learn more [es-dls-overview-search-time-learn-more]

* [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md)
* [Elasticsearch Document Level Security^](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/controlling-access-at-document-field-level.md)

