/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.shards;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
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

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId(new Index(randomAlphaOfLength(5), UUID.randomUUID().toString()), randomIntBetween(0, 5)),
                null, randomBoolean(), ShardRoutingState.UNASSIGNED);
        shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted();
        shardRouting = shardRouting.relocate("node-1", ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

        ShardMonitoringDoc doc = new ShardMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5), 1437580442979L,
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                shardRouting,
                UUID.randomUUID().toString());
        return doc;
    }

    public void testShardsResolver() throws Exception {
        ShardMonitoringDoc doc = newMonitoringDoc();

        ShardsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
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
}
