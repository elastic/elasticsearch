/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.stream.Collectors;

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
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                    "#remove-http-enabled",
                "nodes with http.enabled set: " + nodesFound);
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                    "#_index_thread_pool",
                "nodes with index thread pool settings: " + nodesFound);
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                    "#_tribe_node_removed",
                "nodes with tribe node settings: " + nodesFound);
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
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
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
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
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
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                    "#_file_based_discovery_plugin",
                "nodes with discovery-file installed: " + nodesFound);
        }
        return null;
    }
}
