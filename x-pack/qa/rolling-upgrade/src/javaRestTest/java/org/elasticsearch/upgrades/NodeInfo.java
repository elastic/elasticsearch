/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.RollingUpgradePerformer;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

record NodeInfo(
    String nodeId,
    String version,
    String buildHash,
    TransportVersion transportVersion,
    Set<String> features,
    String restEndpoint
) {

    boolean isOriginalVersionCluster() {
        return RollingUpgradePerformer.isOldClusterVersion(version, buildHash);
    }

    boolean isUpgradedVersionCluster() {
        return isOriginalVersionCluster() == false;
    }

    boolean supportsFeature(String feature) {
        return features.contains(feature);
    }

    @SuppressWarnings("unchecked")
    static List<NodeInfo> getAll(RestClient client) throws IOException {
        final Request clusterStateRequest = new Request("GET", "_cluster/state");
        clusterStateRequest.addParameter("filter_path", "nodes_features");
        final List<?> nodesFeaturesList = ObjectPath.createFromResponse(client.performRequest(clusterStateRequest))
            .evaluate("nodes_features");

        final Map<String, Set<String>> nodeFeatures;
        if (nodesFeaturesList != null) {
            nodeFeatures = nodesFeaturesList.stream()
                .map(Map.class::cast)
                .collect(
                    Collectors.toUnmodifiableMap(m -> m.get("node_id").toString(), m -> new HashSet<>((List<String>) m.get("features")))
                );
        } else {
            nodeFeatures = Map.of();
        }

        final Map<String, Object> nodes = ObjectPath.createFromResponse(client.performRequest(new Request("GET", "_nodes/_all")))
            .evaluate("nodes");
        assertThat("Nodes info is null", nodes, notNullValue());

        return nodes.entrySet().stream().map(entry -> {
            final Map<?, ?> info = (Map<?, ?>) entry.getValue();
            final String version = (String) extractValue(info, "version");
            final String buildHash = (String) extractValue(info, "build_hash");
            final Number tvId = (Number) extractValue(info, "transport_version");
            final TransportVersion transportVersion = tvId != null ? TransportVersion.fromId(tvId.intValue()) : TransportVersion.zero();
            @SuppressWarnings("unchecked")
            final Map<String, Object> httpInfo = (Map<String, Object>) extractValue(info, "http");
            final String restEndpoint = httpInfo != null ? (String) httpInfo.get("publish_address") : null;
            return new NodeInfo(
                entry.getKey(),
                version,
                buildHash,
                transportVersion,
                nodeFeatures.getOrDefault(entry.getKey(), Set.of()),
                restEndpoint
            );
        }).toList();
    }
}
