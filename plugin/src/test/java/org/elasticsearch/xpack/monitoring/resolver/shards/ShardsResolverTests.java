/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.shards;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class ShardsResolverTests extends MonitoringIndexNameResolverTestCase<ShardMonitoringDoc, ShardsResolver> {

    @Override
    protected ShardMonitoringDoc newMonitoringDoc() {
        ShardMonitoringDoc doc = new ShardMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setClusterStateUUID(UUID.randomUUID().toString());
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId(new Index(randomAsciiOfLength(5), UUID.randomUUID().toString()), randomIntBetween(0, 5)),
                null, randomBoolean(), ShardRoutingState.UNASSIGNED);
        shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted();
        shardRouting = shardRouting.relocate("node-1", ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        doc.setShardRouting(shardRouting);
        return doc;
    }

    public void testShardsResolver() throws Exception {
        ShardMonitoringDoc doc = newMonitoringDoc();
        doc.setTimestamp(1437580442979L);

        final String clusterStateUUID = UUID.randomUUID().toString();
        doc.setClusterStateUUID(clusterStateUUID);
        doc.setSourceNode(new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));

        ShardsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ShardsResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(ShardsResolver.id(clusterStateUUID, doc.getShardRouting())));
        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "state_uuid",
                        "shard.state",
                        "shard.primary",
                        "shard.node",
                        "shard.relocating_node",
                        "shard.shard",
                        "shard.index"), XContentType.JSON);

        final String index = "test-" + randomIntBetween(0, 100);
        final int shardId = randomIntBetween(0, 500);
        final boolean primary = randomBoolean();
        final String sourceNode = "node-" + randomIntBetween(0, 5);
        final String relocationNode = "node-" + randomIntBetween(6, 10);

        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId(new Index(index, ""), shardId),
                null, primary, ShardRoutingState.UNASSIGNED);
        shardRouting = shardRouting.initialize(sourceNode, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted();
        shardRouting = shardRouting.relocate(relocationNode, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        doc.setShardRouting(shardRouting);

        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ShardsResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(ShardsResolver.id(clusterStateUUID, shardRouting)));
        assertThat(resolver.id(doc),
                equalTo(clusterStateUUID + ":" + sourceNode + ":" + index + ":" + shardId + ":" + (primary ? "p" : "r")));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "state_uuid",
                        "shard.state",
                        "shard.primary",
                        "shard.node",
                        "shard.relocating_node",
                        "shard.shard",
                        "shard.index"), XContentType.JSON);
    }

    public void testShardId() {
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(new Index("bar", ""), 42), false,
                RecoverySource.PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(ShardsResolver.id("foo", shardRouting), equalTo("foo:_na:bar:42:r"));
        shardRouting = shardRouting.initialize("node1", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted();
        assertThat(ShardsResolver.id("foo", shardRouting), equalTo("foo:node1:bar:42:r"));
    }
}
