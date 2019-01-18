/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

/**
 * Node-specific deprecation checks
 */
public class NodeDeprecationChecks {

    static DeprecationIssue httpEnabledSettingRemoved(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().hasValue(NetworkModule.HTTP_ENABLED.getKey()))
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP Enabled setting removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#remove-http-enabled",
                "nodes with http.enabled set: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue auditLogPrefixSettingsCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getByPrefix("xpack.security.audit.logfile.prefix").isEmpty() == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Audit log node info settings renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#audit-logfile-local-node-info",
                "nodes with audit log settings that have been renamed: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue indexThreadPoolCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getByPrefix("thread_pool.index.").isEmpty() == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Index thread pool removed in favor of combined write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_index_thread_pool",
                "nodes with index thread pool settings: " + nodesFound);
        }
        return null;
    }
    static DeprecationIssue bulkThreadPoolCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getByPrefix("thread_pool.bulk.").isEmpty() == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Bulk thread pool renamed to write thread pool",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#write-thread-pool-fallback",
                "nodes with bulk thread pool settings: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue tribeNodeCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getByPrefix("tribe.").isEmpty() == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Tribe Node removed in favor of Cross Cluster Search",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_tribe_node_removed",
                "nodes with tribe node settings: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue authRealmsTypeCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getGroups("xpack.security.authc.realms").size() > 0)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());

        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Security realm settings structure changed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#include-realm-type-in-setting",
                "nodes have authentication realm configuration which must be updated at time of upgrade to 7.0: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue httpPipeliningCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().hasValue(HttpTransportSettings.SETTING_PIPELINING.getKey()))
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "HTTP pipelining setting removed as pipelining is now mandatory",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#remove-http-pipelining-setting",
                "nodes with http.pipelining set: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue discoveryConfigurationCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {

        List<String> nodesFound = nodeInfos.stream()
            // These checks only apply in Zen2, which is the new default in 7.0 and can't be used in 6.x, so only apply the checks if this
            // node does not have a discovery type explicitly set
            .filter(nodeInfo -> nodeInfo.getSettings().hasValue(DISCOVERY_TYPE_SETTING.getKey()) == false)
            // This only checks for `ping.unicast.hosts` and `hosts_provider` because `cluster.initial_master_nodes` does not exist in 6.x
            .filter(nodeInfo -> nodeInfo.getSettings().hasValue(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()) == false)
            .filter(nodeInfo -> nodeInfo.getSettings().hasValue(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey()) == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Discovery configuration is required in production mode",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_discovery_configuration_is_required_in_production",
                "nodes which do not have discovery configured: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue watcherNotificationsSecureSettingsCheck(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream().filter(nodeInfo ->
                        (false == nodeInfo.getSettings().getByPrefix("xpack.notification.email.account.")
                            .filter(s -> s.endsWith(".smtp.password")).isEmpty())
                        || (false == nodeInfo.getSettings().getByPrefix("xpack.notification.hipchat.account.")
                                .filter(s -> s.endsWith(".auth_token")).isEmpty())
                        || (false == nodeInfo.getSettings().getByPrefix("xpack.notification.jira.account.")
                                .filter(s -> s.endsWith(".url") || s.endsWith(".user") || s.endsWith(".password")).isEmpty())
                        || (false == nodeInfo.getSettings().getByPrefix("xpack.notification.pagerduty.account.")
                                .filter(s -> s.endsWith(".service_api_key")).isEmpty())
                        || (false == nodeInfo.getSettings().getByPrefix("xpack.notification.slack.account.").filter(s -> s.endsWith(".url"))
                                .isEmpty()))
                .map(nodeInfo -> nodeInfo.getNode().getName()).collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "Watcher notification accounts' authentication settings must be defined securely",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                        "#watcher-notifications-account-settings",
                    "nodes which have insecure notification account settings are: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue azureRepositoryChanges(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo ->
                nodeInfo.getPlugins().getPluginInfos().stream()
                    .anyMatch(pluginInfo -> "repository-azure".equals(pluginInfo.getName()))
            ).map(nodeInfo -> nodeInfo.getNode().getName()).collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Azure Repository settings changed",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_azure_repository_plugin",
                "nodes with repository-azure installed: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue gcsRepositoryChanges(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo ->
                nodeInfo.getPlugins().getPluginInfos().stream()
                    .anyMatch(pluginInfo -> "repository-gcs".equals(pluginInfo.getName()))
            ).map(nodeInfo -> nodeInfo.getNode().getName()).collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "GCS Repository settings changed",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_google_cloud_storage_repository_plugin",
                "nodes with repository-gcs installed: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue fileDiscoveryPluginRemoved(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo ->
                nodeInfo.getPlugins().getPluginInfos().stream()
                    .anyMatch(pluginInfo -> "discovery-file".equals(pluginInfo.getName()))
            ).map(nodeInfo -> nodeInfo.getNode().getName()).collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "File-based discovery is no longer a plugin and uses a different path",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#_file_based_discovery_plugin",
                "nodes with discovery-file installed: " + nodesFound);
        }
        return null;
    }

    static DeprecationIssue defaultSSLSettingsRemoved(List<NodeInfo> nodeInfos, List<NodeStats> nodeStats) {
        List<String> nodesFound = nodeInfos.stream()
            .filter(nodeInfo -> nodeInfo.getSettings().getByPrefix("xpack.ssl").isEmpty() == false)
            .map(nodeInfo -> nodeInfo.getNode().getName())
            .collect(Collectors.toList());
        if (nodesFound.size() > 0) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Default TLS/SSL settings have been removed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#tls-setting-fallback",
                "Nodes with default TLS/SSL settings: " + nodesFound);
        }
        return null;
    }
}
