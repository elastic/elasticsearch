/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions.valuesampling;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Unit tests for the pure, cluster-state-driven pieces of {@link HotTierValueSampler} (see the
 * suggestions API spec) that don't need a live cluster: mapping-type resolution and hot-tier node
 * bundling. The fan-out/merge/security logic needs a running node and is covered by
 * {@code EsqlSuggestionsActionIT}-style integration coverage instead.
 */
public class HotTierValueSamplerTests extends ESTestCase {

    public void testIsPlainKeywordMappingAcceptsPlainKeyword() {
        ProjectMetadata metadata = projectWithMapping("""
            { "properties": { "status": { "type": "keyword" } } }
            """);
        assertTrue(HotTierValueSampler.isPlainKeywordMapping(metadata, "test", "status"));
    }

    public void testIsPlainKeywordMappingRejectsConstantKeyword() {
        ProjectMetadata metadata = projectWithMapping("""
            { "properties": { "status": { "type": "constant_keyword" } } }
            """);
        assertFalse(HotTierValueSampler.isPlainKeywordMapping(metadata, "test", "status"));
    }

    public void testIsPlainKeywordMappingRejectsWildcard() {
        ProjectMetadata metadata = projectWithMapping("""
            { "properties": { "status": { "type": "wildcard" } } }
            """);
        assertFalse(HotTierValueSampler.isPlainKeywordMapping(metadata, "test", "status"));
    }

    public void testIsPlainKeywordMappingRejectsMissingField() {
        ProjectMetadata metadata = projectWithMapping("""
            { "properties": { "status": { "type": "keyword" } } }
            """);
        assertFalse(HotTierValueSampler.isPlainKeywordMapping(metadata, "test", "nope"));
    }

    public void testIsPlainKeywordMappingResolvesDottedPath() {
        ProjectMetadata metadata = projectWithMapping("""
            { "properties": { "g": { "properties": { "country_iso_code": { "type": "keyword" } } } } }
            """);
        assertTrue(HotTierValueSampler.isPlainKeywordMapping(metadata, "test", "g.country_iso_code"));
    }

    public void testResolveNodeBundlesOnlyIncludesHotNodes() {
        DiscoveryNode hotNode = discoveryNode("hot-node", Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        DiscoveryNode warmOnlyNode = discoveryNode("warm-node", Set.of(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(hotNode).add(warmOnlyNode).localNodeId("hot-node").build();

        Index index = new Index("test", "uuid");
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "hot-node", true, ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 1), "warm-node", true, ShardRoutingState.STARTED))
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        HotTierValueSampler.NodeBundleResult result = HotTierValueSampler.resolveNodeBundles(routingTable, nodes, Set.of("test"), true);

        assertMap(result.bundles(), matchesMap().entry("hot-node", containsInAnyOrder(new ShardId(index, 0))));
        assertFalse(result.coldSkipped());
    }

    public void testResolveNodeBundlesEmptyWhenNoHotNode() {
        DiscoveryNode warmOnlyNode = discoveryNode("warm-node", Set.of(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(warmOnlyNode).localNodeId("warm-node").build();

        Index index = new Index("test", "uuid");
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "warm-node", true, ShardRoutingState.STARTED))
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        HotTierValueSampler.NodeBundleResult result = HotTierValueSampler.resolveNodeBundles(routingTable, nodes, Set.of("test"), true);

        assertMap(result.bundles(), matchesMap());
        // A warm-only copy is neither hot nor cold, so this isn't a skip_cold concern.
        assertFalse(result.coldSkipped());
    }

    public void testResolveNodeBundlesTreatsGenericDataRoleAsHot() {
        // A single-tier (non-tiered) deployment's plain "data" role node counts as hot too — see
        // DataTier#isHotNode and the single-node-cluster YAML acceptance case.
        DiscoveryNode genericDataNode = discoveryNode("data-node", Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(genericDataNode).localNodeId("data-node").build();

        Index index = new Index("test", "uuid");
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "data-node", true, ShardRoutingState.STARTED))
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        HotTierValueSampler.NodeBundleResult result = HotTierValueSampler.resolveNodeBundles(routingTable, nodes, Set.of("test"), true);

        assertMap(result.bundles(), matchesMap().entry("data-node", containsInAnyOrder(new ShardId(index, 0))));
    }

    public void testResolveNodeBundlesSkipsColdIndexAndReportsColdSkipped() {
        DiscoveryNode coldNode = discoveryNode("cold-node", Set.of(DiscoveryNodeRole.DATA_COLD_NODE_ROLE));
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(coldNode).localNodeId("cold-node").build();

        Index index = new Index("test", "uuid");
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "cold-node", true, ShardRoutingState.STARTED))
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        HotTierValueSampler.NodeBundleResult result = HotTierValueSampler.resolveNodeBundles(routingTable, nodes, Set.of("test"), true);

        assertMap(result.bundles(), matchesMap());
        assertTrue(result.coldSkipped());
    }

    public void testResolveNodeBundlesIncludesColdIndexWhenSkipColdIsFalse() {
        DiscoveryNode coldNode = discoveryNode("cold-node", Set.of(DiscoveryNodeRole.DATA_COLD_NODE_ROLE));
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(coldNode).localNodeId("cold-node").build();

        Index index = new Index("test", "uuid");
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.newShardRouting(new ShardId(index, 0), "cold-node", true, ShardRoutingState.STARTED))
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        HotTierValueSampler.NodeBundleResult result = HotTierValueSampler.resolveNodeBundles(routingTable, nodes, Set.of("test"), false);

        assertMap(result.bundles(), matchesMap().entry("cold-node", containsInAnyOrder(new ShardId(index, 0))));
        assertFalse(result.coldSkipped());
    }

    private static ProjectMetadata projectWithMapping(String mappingJson) {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(mappingJson)
            .build();
        return ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, false).build();
    }

    private static DiscoveryNode discoveryNode(String id, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.create(id, buildNewFakeTransportAddress(), Map.of(), roles);
    }
}
