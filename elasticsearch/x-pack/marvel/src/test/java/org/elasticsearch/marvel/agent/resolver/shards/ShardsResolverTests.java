/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.shards;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingTestUtils;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.marvel.agent.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;

import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class ShardsResolverTests extends MonitoringIndexNameResolverTestCase<ShardMonitoringDoc, ShardsResolver> {

    @Override
    protected ShardMonitoringDoc newMarvelDoc() {
        ShardMonitoringDoc doc = new ShardMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setClusterStateUUID(UUID.randomUUID().toString());
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));

        ShardRouting shardRouting = ShardRouting.newUnassigned(new Index(randomAsciiOfLength(5), UUID.randomUUID().toString()),
                randomIntBetween(0, 5), null, randomBoolean(), new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        ShardRoutingTestUtils.initialize(shardRouting, "node-0");
        ShardRoutingTestUtils.moveToStarted(shardRouting);
        ShardRoutingTestUtils.relocate(shardRouting, "node-1");
        doc.setShardRouting(shardRouting);
        return doc;
    }

    public void testShardsResolver() throws Exception {
        ShardMonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L);

        final String clusterStateUUID = UUID.randomUUID().toString();
        doc.setClusterStateUUID(clusterStateUUID);
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));

        ShardsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ShardsResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(ShardsResolver.id(clusterStateUUID, doc.getShardRouting())));
        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "state_uuid",
                "shard.state",
                "shard.primary",
                "shard.node",
                "shard.relocating_node",
                "shard.shard",
                "shard.index");

        final String index = "test-" + randomIntBetween(0, 100);
        final int shardId = randomIntBetween(0, 500);
        final boolean primary = randomBoolean();
        final String sourceNode = "node-" + randomIntBetween(0, 5);
        final String relocationNode = "node-" + randomIntBetween(6, 10);

        final ShardRouting shardRouting = ShardRouting.newUnassigned(new Index(index, ""), shardId, null, primary,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        ShardRoutingTestUtils.initialize(shardRouting, sourceNode);
        ShardRoutingTestUtils.moveToStarted(shardRouting);
        ShardRoutingTestUtils.relocate(shardRouting, relocationNode);
        doc.setShardRouting(shardRouting);

        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ShardsResolver.TYPE));
        assertThat(resolver.id(doc), equalTo(ShardsResolver.id(clusterStateUUID, shardRouting)));
        assertThat(resolver.id(doc),
                equalTo(clusterStateUUID + ":" + sourceNode + ":" + index + ":" + shardId + ":" + (primary ? "p" : "r")));

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "state_uuid",
                "shard.state",
                "shard.primary",
                "shard.node",
                "shard.relocating_node",
                "shard.shard",
                "shard.index");
    }

    public void testShardId() {
        ShardRouting shardRouting = ShardRouting.newUnassigned(new Index("bar", ""), 42, null, false,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(ShardsResolver.id("foo", shardRouting), equalTo("foo:_na:bar:42:r"));
        ShardRoutingTestUtils.initialize(shardRouting, "node1");
        ShardRoutingTestUtils.moveToStarted(shardRouting);
        assertThat(ShardsResolver.id("foo", shardRouting), equalTo("foo:node1:bar:42:r"));
    }
}
