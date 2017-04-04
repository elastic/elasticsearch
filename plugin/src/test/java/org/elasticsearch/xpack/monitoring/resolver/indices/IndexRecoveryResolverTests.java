/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexRecoveryResolverTests extends MonitoringIndexNameResolverTestCase<IndexRecoveryMonitoringDoc, IndexRecoveryResolver> {

    @Override
    protected IndexRecoveryMonitoringDoc newMonitoringDoc() {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Map<String, java.util.List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId("test", "uuid", 0), localNode.getId(), true,
                ShardRoutingState.INITIALIZING, RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE);
        shardRecoveryStates.put("test", Collections.singletonList(new RecoveryState(shardRouting, localNode, null)));

        IndexRecoveryMonitoringDoc doc = new IndexRecoveryMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5), 1437580442979L,
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                new RecoveryResponse(10, 10, 0, false, shardRecoveryStates, Collections.emptyList()));
        return doc;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testIndexRecoveryResolver() throws Exception {
        IndexRecoveryMonitoringDoc doc = newMonitoringDoc();

        IndexRecoveryResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "source_node",
                        "index_recovery"), XContentType.JSON);
    }
}
