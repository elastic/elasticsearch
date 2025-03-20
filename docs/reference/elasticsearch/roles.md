---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/built-in-roles.html
---

# Roles [built-in-roles]

:::{note}
This section provides detailed **reference information** for Elasticsearch privileges.

Refer to [User roles](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/user-roles.md) in the **Deploy and manage** section for overview, getting started and conceptual information.
:::

The {{stack-security-features}} apply a default role to all users, including [anonymous users](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/anonymous-access.md). The default role enables users to access the authenticate endpoint, change their own passwords, and get information about themselves.

There is also a set of built-in roles you can explicitly assign to users. These roles have a fixed set of privileges and cannot be updated.

$$$built-in-roles-apm-system$$$ `apm_system`
:   Grants access necessary for the APM system user to send system-level data (such as monitoring) to {{es}}.

$$$built-in-roles-beats-admin$$$ `beats_admin`
:   Grants access to the `.management-beats` index, which contains configuration information for the Beats.

$$$built-in-roles-beats-system$$$ `beats_system`
:   Grants access necessary for the Beats system user to send system-level data (such as monitoring) to {{es}}.

    ::::{note}
    * This role should not be assigned to users as the granted permissions may change between releases.
    * This role does not provide access to the beats indices and is not suitable for writing beats output to {{es}}.

    ::::


$$$built-in-roles-editor$$$ `editor`
:   Grants full access to all features in {{kib}} (including Solutions) and read-only access to data indices.

    ::::{note}
    * This role provides read access to any index that is not prefixed with a dot.
    * This role automatically grants full access to new {{kib}} features as soon as they are released.
    * Some {{kib}} features may also require creation or write access to data indices. {{ml-cap}} {{dfanalytics-jobs}} is an example. For such features those privileges must be defined in a separate role.

    ::::


$$$built-in-roles-enrich-user$$$ `enrich_user`
:   Grants access to manage **all** enrich indices (`.enrich-*`) and **all** operations on ingest pipelines.

$$$built-in-roles-inference-admin$$$ `inference_admin`
:   Provides all of the privileges of the `inference_user` role and the full use of the {{infer-cap}} APIs. Grants the `manage_inference` cluster privilege.

$$$built-in-roles-inference-user$$$ `inference_user`
:   Provides the minimum privileges required to view {{infer}} configurations and perform inference. Grants the `monintor_inference` cluster privilege.

$$$built-in-roles-ingest-user$$$ `ingest_admin`
:   Grants access to manage **all** index templates and **all** ingest pipeline configurations.

    ::::{note}
    This role does **not** provide the ability to create indices; those privileges must be defined in a separate role.
    ::::


$$$built-in-roles-kibana-dashboard$$$ `kibana_dashboard_only_user`
:   (This role is deprecated, please use [{{kib}} feature privileges](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges.md#kibana-feature-privileges) instead). Grants read-only access to the {{kib}} Dashboard in every [space in {{kib}}](docs-content://deploy-manage/manage-spaces.md). This role does not have access to editing tools in {{kib}}.

$$$built-in-roles-kibana-system$$$ `kibana_system`
:   Grants access necessary for the {{kib}} system user to read from and write to the {{kib}} indices, manage index templates and tokens, and check the availability of the {{es}} cluster. It also permits activating, searching, and retrieving user profiles, as well as updating user profile data for the `kibana-*` namespace. This role grants read access to the `.monitoring-*` indices and read and write access to the `.reporting-*` indices. For more information, see [Configuring Security in {{kib}}](docs-content://deploy-manage/security/secure-your-cluster-deployment.md).

    ::::{note}
    This role should not be assigned to users as the granted permissions may change between releases.
    ::::


$$$built-in-roles-kibana-admin$$$ `kibana_admin`
:   Grants access to all features in {{kib}}. For more information on {{kib}} authorization, see [Kibana authorization](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-roles.md).

$$$built-in-roles-kibana-user$$$ `kibana_user`
:   (This role is deprecated, please use the [`kibana_admin`](#built-in-roles-kibana-admin) role instead.) Grants access to all features in {{kib}}. For more information on {{kib}} authorization, see [Kibana authorization](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-roles.md).

$$$built-in-roles-logstash-admin$$$ `logstash_admin`
:   Grants access to the `.logstash*` indices for managing configurations, and grants necessary access for logstash-specific APIs exposed by the logstash x-pack plugin.

$$$built-in-roles-logstash-system$$$ `logstash_system`
:   Grants access necessary for the Logstash system user to send system-level data (such as monitoring) to {{es}}. For more information, see [Configuring Security in Logstash](logstash://reference/secure-connection.md).

    ::::{note}
    * This role should not be assigned to users as the granted permissions may change between releases.
    * This role does not provide access to the logstash indices and is not suitable for use within a Logstash pipeline.

    ::::


$$$built-in-roles-ml-admin$$$ `machine_learning_admin`
:   Provides all of the privileges of the `machine_learning_user` role plus the full use of the {{ml}} APIs. Grants `manage_ml` cluster privileges, read access to `.ml-anomalies*`, `.ml-notifications*`, `.ml-state*`, `.ml-meta*` indices and write access to `.ml-annotations*` indices. {{ml-cap}} administrators also need index privileges for source and destination indices and roles that grant access to {{kib}}. See [{{ml-cap}} security privileges](docs-content://explore-analyze/machine-learning/setting-up-machine-learning.md#setup-privileges).

$$$built-in-roles-ml-user$$$ `machine_learning_user`
:   Grants the minimum privileges required to view {{ml}} configuration, status, and work with results. This role grants `monitor_ml` cluster privileges, read access to the `.ml-notifications` and `.ml-anomalies*` indices (which store {{ml}} results), and write access to `.ml-annotations*` indices. {{ml-cap}} users also need index privileges for source and destination indices and roles that grant access to {{kib}}. See [{{ml-cap}} security privileges](docs-content://explore-analyze/machine-learning/setting-up-machine-learning.md#setup-privileges).

$$$built-in-roles-monitoring-user$$$ `monitoring_user`
:   Grants the minimum privileges required for any user of {{monitoring}} other than those required to use {{kib}}. This role grants access to the monitoring indices and grants privileges necessary for reading basic cluster information. This role also includes all [Kibana privileges](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges.md) for the {{stack-monitor-features}}. Monitoring users should also be assigned the `kibana_admin` role, or another role with [access to the {{kib}} instance](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-roles.md).

$$$built-in-roles-remote-monitoring-agent$$$ `remote_monitoring_agent`
:   Grants the minimum privileges required to write data into the monitoring indices (`.monitoring-*`). This role also has the privileges necessary to create {{metricbeat}} indices (`metricbeat-*`) and write data into them.

$$$built-in-roles-remote-monitoring-collector$$$ `remote_monitoring_collector`
:   Grants the minimum privileges required to collect monitoring data for the {{stack}}.

$$$built-in-roles-reporting-user$$$ `reporting_user`
:   Grants the necessary privileges required to use {{report-features}} in {{kib}}, including generating and downloading reports. This role implicitly grants access to all Kibana reporting features, with each user having access only to their own reports. Note that reporting users should also be assigned additional roles that grant read access to the [indices](https://www.elastic.co/guide/en/elasticsearch/reference/current/defining-roles.html#roles-indices-priv) that will be used to generate reports.

$$$built-in-roles-rollup-admin$$$ `rollup_admin`
:   Grants `manage_rollup` cluster privileges, which enable you to manage and execute all rollup actions.

$$$built-in-roles-rollup-user$$$ `rollup_user`
:   Grants `monitor_rollup` cluster privileges, which enable you to perform read-only operations related to rollups.

$$$built-in-roles-snapshot-user$$$ `snapshot_user`
:   Grants the necessary privileges to create snapshots of **all** the indices and to view their metadata. This role enables users to view the configuration of existing snapshot repositories and snapshot details. It does not grant authority to remove or add repositories or to restore snapshots. It also does not enable to change index settings or to read or update data stream or index data.

$$$built-in-roles-superuser$$$ `superuser`
:   Grants full access to cluster management and data indices. This role also grants direct read-only access to restricted indices like `.security`. A user with the `superuser` role can [impersonate](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/submitting-requests-on-behalf-of-other-users.md) any other user in the system.

    On {{ecloud}}, all standard users, including those with the `superuser` role are restricted from performing [operator-only](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/operator-only-functionality.md) actions.

    ::::{important}
    This role can manage security and create roles with unlimited privileges. Take extra care when assigning it to a user.
    ::::


$$$built-in-roles-transform-admin$$$ `transform_admin`
:   Grants `manage_transform` cluster privileges, which enable you to manage {{transforms}}. This role also includes all [Kibana privileges](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges.md) for the {{ml-features}}.

$$$built-in-roles-transform-user$$$ `transform_user`
:   Grants `monitor_transform` cluster privileges, which enable you to perform read-only operations related to {{transforms}}. This role also includes all [Kibana privileges](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges.md) for the {{ml-features}}.

$$$built-in-roles-transport-client$$$ `transport_client`
:   Grants the privileges required to access the cluster through the Java Transport Client. The Java Transport Client fetches information about the nodes in the cluster using the *Node Liveness API* and the *Cluster State API* (when sniffing is enabled). Assign your users this role if they use the Transport Client.

    ::::{note}
    Using the Transport Client effectively means the users are granted access to the cluster state. This means users can view the metadata over all indices, index templates, mappings, node and basically everything about the cluster. However, this role does not grant permission to view the data in all indices.
    ::::


$$$built-in-roles-viewer$$$ `viewer`
:   Grants read-only access to all features in {{kib}} (including Solutions) and to data indices.

    ::::{note}
    * This role provides read access to any index that is not prefixed with a dot.
    * This role automatically grants read-only access to new {{kib}} features as soon as they are available.

    ::::


$$$built-in-roles-watcher-admin$$$ `watcher_admin`
:   Allows users to create and execute all {{watcher}} actions. Grants read access to the `.watches` index. Also grants read access to the watch history and the triggered watches index.


$$$built-in-roles-watcher-user$$$ `watcher_user`
:   Grants read access to the `.watches` index, the get watch action and the watcher stats.
