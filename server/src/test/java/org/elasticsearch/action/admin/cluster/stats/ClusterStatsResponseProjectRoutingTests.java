/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStatsTests;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the project_routing and tags block assembly in ClusterStatsResponse.toXContent().
 * Focused on the gating logic (totalQueries > 0, tagsConfig != null) and the
 * top-level queries sum that is computed at render time rather than stored.
 */
public class ClusterStatsResponseProjectRoutingTests extends ESTestCase {

    public void testProjectRoutingBlock_absent_when_allCountersZero() throws Exception {
        ObjectPath json = toObjectPath(buildResponse(new ProjectRoutingUsageSnapshot(), null));
        assertThat(json.evaluate("project_routing"), nullValue());
    }

    public void testProjectRoutingBlock_present_with_correct_queries_sum() throws Exception {
        // searchQueriesTotal=5, esqlQueriesTotal=3 → queries=8
        ProjectRoutingUsageSnapshot snapshot = new ProjectRoutingUsageSnapshot(5L, 0L, 0L, 0L, 0L, 0L, 0L, 3L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        ObjectPath json = toObjectPath(buildResponse(snapshot, null));
        assertThat(json.evaluate("project_routing.queries"), equalTo(8));
        assertThat(json.evaluate("tags"), nullValue());
    }

    public void testTagsBlock_present_and_independent_of_project_routing() throws Exception {
        // zero-query snapshot — project_routing block must be absent
        ProjectRoutingUsageSnapshot snapshot = new ProjectRoutingUsageSnapshot();
        TagsConfigSnapshot tagsConfig = new TagsConfigSnapshot(List.of("_alias", "mytag"), List.of());
        ObjectPath json = toObjectPath(buildResponse(snapshot, tagsConfig));
        assertThat(json.evaluate("tags.total"), equalTo(2));
        assertThat(json.evaluate("project_routing"), nullValue());
    }

    public void testBothBlocks_present_when_both_non_empty() throws Exception {
        ProjectRoutingUsageSnapshot snapshot = new ProjectRoutingUsageSnapshot(10L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        TagsConfigSnapshot tagsConfig = new TagsConfigSnapshot(List.of("_alias"), List.of());
        ObjectPath json = toObjectPath(buildResponse(snapshot, tagsConfig));
        assertThat(json.evaluate("project_routing.queries"), equalTo(10));
        assertThat(json.evaluate("tags.total"), equalTo(1));
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private static ObjectPath toObjectPath(ClusterStatsResponse response) throws Exception {
        XContentType xContentType = XContentType.JSON;
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return ObjectPath.createFromXContent(xContentType.xContent(), BytesReference.bytes(builder));
        }
    }

    private static ClusterStatsResponse buildResponse(ProjectRoutingUsageSnapshot snapshot, TagsConfigSnapshot tagsConfig) {
        ClusterStatsNodeResponse nodeResponse = buildNodeResponse(snapshot);
        List<ClusterStatsNodeResponse> nodes = List.of(nodeResponse);
        return new ClusterStatsResponse(
            0L,
            "test-uuid",
            new ClusterName("test"),
            nodes,
            List.of(),
            MappingStats.of(Metadata.EMPTY_METADATA, () -> {}),
            AnalysisStats.of(Metadata.EMPTY_METADATA, () -> {}),
            VersionStats.of(Metadata.EMPTY_METADATA, nodes),
            ClusterSnapshotStats.EMPTY,
            Map.of(),
            false,
            tagsConfig
        );
    }

    private static ClusterStatsNodeResponse buildNodeResponse(ProjectRoutingUsageSnapshot snapshot) {
        var node = DiscoveryNodeUtils.create("node1", buildNewFakeTransportAddress());
        TransportAddress addr = buildNewFakeTransportAddress();
        var boundAddr = new BoundTransportAddress(new TransportAddress[] { addr }, addr);
        var osInfo = new OsInfo(0L, 1, Processors.of(1.0), "test", "test", "test", "test");
        var nodeInfo = new NodeInfo(
            Build.current().version(),
            new CompatibilityVersions(TransportVersion.current(), Map.of()),
            IndexVersion.current(),
            Map.of(),
            Build.current(),
            node,
            Settings.EMPTY,
            osInfo,
            null,
            JvmInfo.jvmInfo(),
            null,
            new TransportInfo(boundAddr, Map.of()),
            null,
            null,
            new PluginsAndModules(List.of(), List.of()),
            null,
            null,
            null
        );
        return new ClusterStatsNodeResponse(
            node,
            ClusterHealthStatus.GREEN,
            nodeInfo,
            NodeStatsTests.createNodeStats(),
            new ShardStats[0],
            new SearchUsageStats(),
            RepositoryUsageStats.EMPTY,
            null,
            null,
            snapshot
        );
    }
}
