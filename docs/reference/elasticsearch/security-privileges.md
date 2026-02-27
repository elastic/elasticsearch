---
navigation_title: "Elasticsearch privileges"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/security-privileges.html
applies_to:
  stack: all
  serverless: all
---

# {{es}} privileges [security-privileges]

This section provides detailed **reference information** for Elasticsearch privileges.

If you're using a stack-versioned deployment such as a self-managed cluster, {{ech}}, {{eck}}, or {{ece}}, then refer to [User roles](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/user-roles.md) for more information on how role-based access control works.

If you're using {{serverless-full}}, then refer to [Serverless project custom roles](docs-content://deploy-manage/users-roles/serverless-custom-roles.md) to learn how to build roles using these privileges.

:::{note}
In {{serverless-full}}, Elastic manages the underlying infrastructure for you. Because of this, privileges related to cluster administration, scaling, snapshots, and availability are not available. [Learn more about the features managed by {{serverless-full}}](docs-content://deploy-manage/deploy/elastic-cloud/differences-from-other-elasticsearch-offerings.md).
:::

Roles are governed by a set of configurable privileges grouped into these categories:

* [cluster](#privileges-list-cluster), which you can use to manage core operations like snapshots, managing API keys, autoscaling, and cross-cluster functionality.
* [indices](#privileges-list-indices), which govern document-level access, index and data stream metadata information, and more.
* [run-as](#_run_as_privilege), which allows for secure impersonation.
* [application](#application-privileges), which enable external applications to define and store their privilege models within {{es}} roles.

When creating roles, refer to this page for a complete list of available privileges.

## Cluster privileges [privileges-list-cluster]

`all`
:   All cluster administration operations, like snapshotting, node shutdown/restart, settings update, rerouting, or managing users and roles.

`cancel_task`
:   Privileges to cancel tasks and delete async searches. See [delete async search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-submit) API for more information.

`create_snapshot` {applies_to}`serverless: unavailable`
:   Privileges to create snapshots for existing repositories. Can also list and view details on existing repositories and snapshots.


`cross_cluster_replication` {applies_to}`serverless: unavailable`
:   Privileges to connect to [remote clusters configured with the API key based model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md) for cross-cluster replication.

    ::::{note}
    This privilege must *not* be directly granted. It is used internally by [Create Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and [Update Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-cross-cluster-api-key) to manage cross-cluster API keys.
    ::::


`cross_cluster_search` {applies_to}`serverless: unavailable`
:   Privileges to connect to [remote clusters configured with the API key based model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md) for cross-cluster search.

    ::::{note}
    This privilege must *not* be directly granted. It is used internally by [Create Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and [Update Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-cross-cluster-api-key) to manage cross-cluster API keys.
    ::::


`grant_api_key` {applies_to}`serverless: unavailable`
:   Privileges to create {{es}} API keys on behalf of other users.


`manage`
:   Builds on `monitor` and adds cluster operations that change values in the cluster. This includes snapshotting, updating settings, and rerouting. It also includes obtaining snapshot and restore status. This privilege does not include the ability to manage security.

`manage_api_key`
:   All security-related operations on {{es}} REST API keys including [creating new API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key), [retrieving information about API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-api-key), [querying API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-api-keys), [updating API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-api-key), [bulk updating API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-bulk-update-api-keys), and [invalidating API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-api-key).

    ::::{note}
    * When you create new API keys, they will always be owned by the authenticated user.
    * When you have this privilege, you can invalidate your own API keys and those owned by other users.

    ::::


`manage_autoscaling` {applies_to}`serverless: unavailable`
:   All operations related to managing autoscaling policies.


`manage_ccr` {applies_to}`serverless: unavailable`
:   All {{ccr}} operations related to managing follower indices and auto-follow patterns. It also includes the authority to grant the privileges necessary to manage follower indices and auto-follow patterns. This privilege is necessary only on clusters that contain follower indices.


`manage_data_frame_transforms` {applies_to}`serverless: unavailable` {applies_to}`stack: deprecated 7.5`
:   Use `manage_transform` instead. {{es}} version 7.5 and older used `manage_data_frame_transforms` in operations related to managing {{transforms}}.


`manage_data_stream_global_retention` {applies_to}`stack: deprecated 8.16`
:   This privilege has no effect.

`manage_enrich`
:   All operations related to managing and executing enrich policies.

`manage_ilm` {applies_to}`serverless: unavailable`
:   All {{ilm}} operations related to managing policies.


`manage_index_templates`
:   All operations on index templates.

`manage_inference`
:   All operations related to managing {{infer}}.

`manage_ingest_pipelines`
:   All operations on ingest pipelines.

`manage_logstash_pipelines`
:   All operations on logstash pipelines.

`manage_ml`
:   All {{ml}} operations, such as creating and deleting {{dfeeds}}, jobs, and model snapshots.

    ::::{note}
    {{dfeeds-cap}} that were created prior to version 6.2 or created when {{security-features}} were disabled run as a system user with elevated privileges, including permission to read all indices. Newer {{dfeeds}} run with the security roles of the user who created or updated them.
    ::::


`manage_oidc` {applies_to}`serverless: unavailable`
:   Enables the use of {{es}} APIs ([OpenID connect prepare authentication](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-prepare-authentication), [OpenID connect authenticate](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-authenticate), and [OpenID connect logout](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-logout)) to initiate and manage OpenID Connect authentication on behalf of other users.


`manage_own_api_key`
:   All security-related operations on {{es}} API keys that are owned by the current authenticated user. The operations include [creating new API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key), [retrieving information about API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-api-key), [querying API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-api-keys), [updating API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-api-key), [bulk updating API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-bulk-update-api-keys), and [invalidating API keys](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-api-key).

`manage_pipeline`
:   All operations on ingest pipelines.

`manage_rollup` {applies_to}`serverless: unavailable`
:   All rollup operations, including creating, starting, stopping and deleting rollup jobs.


`manage_saml` {applies_to}`serverless: unavailable`
:   Enables the use of internal {{es}} APIs to initiate and manage SAML authentication on behalf of other users.


`manage_search_application`
:   All CRUD operations on [search applications](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-search_application).

`manage_search_query_rules`
:   All CRUD operations on [query rules](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules).

`manage_search_synonyms`
:   All synonyms management operations on [Synonyms APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-synonyms).

`manage_security`
:   All security-related operations such as CRUD operations on users and roles and cache clearing.

`manage_service_account` {applies_to}`serverless: unavailable`
:   All security-related operations on {{es}} service accounts including [Get service accounts](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-service-accounts), [Create service account tokens](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-service-token), [Delete service account token](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-service-token), and [Get service account credentials](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-service-credentials).


`manage_slm` {applies_to}`serverless: unavailable` {applies_to}`stack: deprecated 8.15`
:   All {{slm}} ({{slm-init}}) actions, including creating and updating policies and starting and stopping {{slm-init}}. It also grants the permission to start and stop {{Ilm}}, using the [ILM start](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-start) and [ILM stop](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-stop) APIs.

    In a future major release, this privilege will not grant any {{Ilm}} permissions.

`manage_token` {applies_to}`serverless: unavailable`
:   All security-related operations on tokens that are generated by the {{es}} Token Service.


`manage_transform`
:   All operations related to managing {{transforms}}.

`manage_watcher` {applies_to}`serverless: unavailable`
:   All watcher operations, such as putting watches, executing, activate or acknowledging.

    ::::{note}
    Watches that were created prior to version 6.1 or created when the {{security-features}} were disabled run as a system user with elevated privileges, including permission to read and write all indices. Newer watches run with the security roles of the user who created or updated them.
    ::::


`monitor`
:   All cluster read-only operations, like cluster health and state, hot threads, node info, node and cluster stats, and pending cluster tasks.

`monitor_data_stream_global_retention` {applies_to}`stack: unavailable, deprecated 8.16`
:   This privilege has no effect.

`monitor_enrich`
:   All read-only operations related to managing and executing enrich policies.

`monitor_esql` {applies_to}`stack: ga 9.1`
:   All read-only operations related to ES|QL queries.

`monitor_inference`
:   All read-only operations related to {{infer}}.

`monitor_ml`
:   All read-only {{ml}} operations, such as getting information about {{dfeeds}}, jobs, model snapshots, or results.

`monitor_rollup` {applies_to}`serverless: unavailable`
:   All read-only rollup operations, such as viewing the list of historical and currently running rollup jobs and their capabilities.


`monitor_snapshot` {applies_to}`serverless: unavailable`
:   Privileges to list and view details on existing repositories and snapshots.


`monitor_stats` {applies_to}`serverless: unavailable`
:   Privileges to list and view details of stats.


`monitor_text_structure` {applies_to}`serverless: unavailable`
:   All read-only operations related to the [find structure API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-text-structure-find-structure).


`monitor_transform`
:   All read-only operations related to {{transforms}}.

`monitor_watcher` {applies_to}`serverless: unavailable`
:   All read-only watcher operations, such as getting a watch and watcher stats.


`read_ccr` {applies_to}`serverless: unavailable`
:   All read-only {{ccr}} operations, such as getting information about indices and metadata for leader indices in the cluster. It also includes the authority to check whether users have the appropriate privileges to follow leader indices. This privilege is necessary only on clusters that contain leader indices.


`read_ilm` {applies_to}`serverless: unavailable`
:   All read-only {{Ilm}} operations, such as getting policies and checking the status of {Ilm}


`read_pipeline`
:   Read-only access to ingest pipeline (get, simulate).

`read_slm` {applies_to}`serverless: unavailable` {applies_to}`stack: deprecated 8.15`
:   All read-only {{slm-init}} actions, such as getting policies and checking the {{slm-init}} status. It also grants the permission to get the {{Ilm}} status, using the [ILM get status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ilm-get-status).

     In a future major release, this privilege will not grant any {{Ilm}} permissions.

`read_security`
:   All read-only security-related operations, such as getting users, user profiles, {{es}} API keys, {{es}} service accounts, roles and role mappings. Allows [querying](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-api-keys) and [retrieving information](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-api-key) on all {{es}} API keys.

`transport_client` {applies_to}`serverless: unavailable`
:   All privileges necessary for a transport client to connect. Required by the remote cluster to enable [{{ccs}}](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md).



## Indices privileges [privileges-list-indices]

`all`
:   Any action on an index or data stream.

`auto_configure`
:   Permits auto-creation of indices and data streams. An auto-create action is the result of an [index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) or [bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk) request that targets a non-existent index or data stream rather than an explicit [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) or [create data stream](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create-data-stream) request. Also permits auto-update of mappings on indices and data streams if they do not contradict existing mappings. An auto-update mapping action is the result of an index or bulk request on an index or data stream that contains new fields that may be mapped rather than an explicit [update mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) request.

`create`
:   Privilege to index documents.

    ::::{note}
    This privilege does not restrict the index operation to the creation of documents but instead restricts API use to the index API. The index API allows a user to overwrite a previously indexed document. See the `create_doc` privilege for an alternative.
    ::::

    :::{important}
    Starting from 8.0, this privilege no longer grants the permission to update index mappings.
    In earlier versions, it implicitly permitted index mapping updates (excluding data stream mappings) via the [updating mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) or through [dynamic field mapping](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).
    Mapping update capabilities will be fully removed in a future major release.
    :::


`create_doc`
:   Privilege to index documents. It does not grant the permission to update or overwrite existing documents.

    ::::{note}
    This privilege relies on the `op_type` of indexing requests ([Index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) and [Bulk](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk)). When ingesting documents as a user who has the `create_doc` privilege (and no higher privilege such as `index` or `write`), you must ensure that *op_type* is set to *create* through one of the following:

    * Explicitly setting the `op_type` in the index or bulk APIs
    * Using the `_create` endpoint for the index API
    * Creating a document with an auto-generated `_id`

    ::::

    :::{important}
    Starting from 8.0, this privilege no longer grants the permission to update index mappings.
    In earlier versions, it implicitly permitted index mapping updates (excluding data stream mappings) via the [updating mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) or through [dynamic field mapping](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).
    Mapping update capabilities will be fully removed in a future major release.
    :::


`create_index` {applies_to}`serverless: unavailable`
:   Privilege to create an index or data stream. A create index request may contain aliases to be added to the index once created. In that case the request requires the `manage` privilege as well, on both the index and the aliases names.

`cross_cluster_replication` {applies_to}`serverless: unavailable`
:   Privileges to perform cross-cluster replication for indices located on [remote clusters configured with the API key based model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md). This privilege should only be used for the `privileges` field of [remote indices privileges](https://www.elastic.co/guide/en/elasticsearch/reference/current/defining-roles.html#roles-remote-indices-priv).


`cross_cluster_replication_internal` {applies_to}`serverless: unavailable`
:   Privileges to perform supporting actions for cross-cluster replication from [remote clusters configured with the API key based model](docs-content://deploy-manage/remote-clusters/remote-clusters-api-key.md).

    ::::{note}
    This privilege must *not* be directly granted. It is used internally by [Create Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key) and [Update Cross-Cluster API key](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-cross-cluster-api-key) to manage cross-cluster API keys.
    ::::


`delete`
:   Privilege to delete documents.

`delete_index`
:   Privilege to delete an index or data stream.

`index`
:   Privilege to index and update documents.

    :::{important}
    Starting from 8.0, this privilege no longer grants the permission to update index mappings.
    In earlier versions, it implicitly permitted index mapping updates (excluding data stream mappings) via the [updating mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) or through [dynamic field mapping](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).
    Mapping update capabilities will be fully removed in a future major release.
    :::

`maintenance`
:   Permits refresh, flush, synced flush and force merge index administration operations. No privilege to read or write index data or otherwise manage the index.

`manage`
:   All `monitor` privileges plus index and data stream administration (aliases, analyze, cache clear, close, delete, exists, flush, mapping, open, field capabilities, force merge, refresh, settings, search shards, validate query).

`manage_data_stream_lifecycle`
:   All [Data stream lifecycle](docs-content://manage-data/lifecycle/data-stream.md) operations relating to reading and managing the built-in lifecycle of a data stream. This includes operations such as adding and removing a lifecycle from a data stream.

`manage_failure_store` {applies_to}`stack: ga 9.1`
:   All `monitor` privileges plus index and data stream administration limited to failure stores only. Applies only to data streams when accessed through the [index component selector syntax](/reference/elasticsearch/rest-apis/api-conventions.md#api-component-selectors).

`manage_follow_index` {applies_to}`serverless: unavailable`
:   All actions that are required to manage the lifecycle of a follower index, which includes creating a follower index, closing it, and converting it to a regular index. This privilege is necessary only on clusters that contain follower indices.


`manage_ilm` {applies_to}`serverless: unavailable`
:   All {{Ilm}} operations relating to managing the execution of policies of an index or data stream. This includes operations such as retrying policies and removing a policy from an index or data stream.


`manage_leader_index` {applies_to}`serverless: unavailable`
:   All actions that are required to manage the lifecycle of a leader index, which includes [forgetting a follower](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ccr-forget-follower). This privilege is necessary only on clusters that contain leader indices.


`monitor`
:   All actions that are required for monitoring (recovery, segments info, index stats and status).

`read`
:   Read-only access to actions (count, explain, get, mget, get indexed scripts, more like this, multi percolate/search/termvector, percolate, scroll, clear_scroll, search, suggest, tv).

`read_cross_cluster` {applies_to}`serverless: unavailable`
:   Read-only access to the search action from a [remote cluster](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md).

`read_failure_store` {applies_to}`stack: ga 9.1`
:   Read-only access to actions performed on a data stream's failure store. Required for access to failure store data (count, explain, get, mget, get indexed scripts, more like this, multi percolate/search/termvector, percolate, scroll, clear_scroll, search, suggest, tv). Applies only to data streams when accessed through the [index component selector syntax](/reference/elasticsearch/rest-apis/api-conventions.md#api-component-selectors).

`view_index_metadata`
:   Read-only access to index and data stream metadata (aliases, exists, field capabilities, field mappings, get index, get data stream, ilm explain, mappings, search shards, settings, validate query). This privilege is available for use primarily by {{kib}} users.

`write`
:   Privilege to perform all write operations to documents, which includes the permission to index, update, and delete documents as well as performing bulk operations, while also allowing to dynamically update the index mapping.

    :::{important}
    Starting from 8.0, this privilege no longer grants the permission to update index mappings.
    In earlier versions, it implicitly permitted index mapping updates (excluding data stream mappings) via the [updating mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) or through [dynamic field mapping](docs-content://manage-data/data-store/mapping/dynamic-mapping.md).
    Mapping update capabilities will be fully removed in a future major release.
    :::

## Run as privilege [_run_as_privilege]
```{applies_to}
serverless: unavailable
```

The `run_as` permission enables an authenticated user to submit requests on behalf of another user. The value can be a user name or a comma-separated list of user names. (You can also specify users as an array of strings or a YAML sequence.) For more information, see [Submitting requests on behalf of other users](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/submitting-requests-on-behalf-of-other-users.md).


## Application privileges [application-privileges]

Application privileges are managed within {{es}} and can be retrieved with the [has privileges API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-has-privileges) and the [get application privileges API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-privileges). They do not, however, grant access to any actions or resources within {{es}}. Their purpose is to enable applications to represent and store their own privilege models within {{es}} roles.

To create application privileges, use the [add application privileges API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-privileges). You can then associate these application privileges with roles, as described in [Defining roles](https://www.elastic.co/guide/en/elasticsearch/reference/current/defining-roles.html).


