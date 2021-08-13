/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.exporter.BaseFilteredMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ShardsMonitoringDocTests extends BaseFilteredMonitoringDocTestCase<ShardMonitoringDoc> {

    private String stateUuid;
    private boolean assignedToNode;
    private ShardRouting shardRouting;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        stateUuid = randomAlphaOfLength(5);
        assignedToNode = randomBoolean();
        node = assignedToNode ? MonitoringTestUtils.randomMonitoringNode(random()) : null;
        shardRouting = newShardRouting(randomAlphaOfLength(5),
                randomIntBetween(0, 5),
                assignedToNode ? node.getUUID() : null,
                randomBoolean(),
                assignedToNode ? INITIALIZING : UNASSIGNED);
    }

    @Override
    protected ShardMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                     MonitoredSystem system, String type, String id) {
        return new ShardMonitoringDoc(cluster, timestamp, interval, node, shardRouting, stateUuid);
    }

    @Override
    protected void assertFilteredMonitoringDoc(final ShardMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(ShardMonitoringDoc.TYPE));
        assertThat(document.getId(), equalTo(ShardMonitoringDoc.id(stateUuid, shardRouting)));

        assertThat(document.getShardRouting(), is(shardRouting));
        if (assignedToNode) {
            assertThat(document.getShardRouting().assignedToNode(), is(true));
            assertThat(document.getNode(), is(node));
        } else {
            assertThat(document.getNode(), nullValue());
        }
    }

    @Override
    protected Set<String> getExpectedXContentFilters() {
        return ShardMonitoringDoc.XCONTENT_FILTERS;
    }

    public void testConstructorShardRoutingMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new ShardMonitoringDoc(cluster, timestamp, interval, node, null, stateUuid));
    }

    public void testConstructorStateUuidMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new ShardMonitoringDoc(cluster, timestamp, interval, node, shardRouting, null));
    }

    public void testIdWithPrimaryShardAssigned() {
        final ShardRouting shardRouting = newShardRouting("_index_0", 123, "_node_0", randomAlphaOfLength(5), true, INITIALIZING);
        assertEquals("_state_uuid_0:_node_0:_index_0:123:p", ShardMonitoringDoc.id("_state_uuid_0", shardRouting));
    }

    public void testIdWithReplicaShardAssigned() {
        final ShardRouting shardRouting = newShardRouting("_index_1", 456, "_node_1", randomAlphaOfLength(5), false, INITIALIZING);
        assertEquals("_state_uuid_1:_node_1:_index_1:456:r", ShardMonitoringDoc.id("_state_uuid_1", shardRouting));
    }

    public void testIdWithPrimaryShardUnassigned() {
        final ShardRouting shardRouting = newShardRouting("_index_2", 789, null, randomAlphaOfLength(5), true, UNASSIGNED);
        assertEquals("_state_uuid_2:_na:_index_2:789:p", ShardMonitoringDoc.id("_state_uuid_2", shardRouting));
    }

    public void testIdWithReplicaShardUnassigned() {
        final ShardRouting shardRouting = newShardRouting("_index_3", 159, null, randomAlphaOfLength(5), false, UNASSIGNED);
        assertEquals("_state_uuid_3:_na:_index_3:159:r", ShardMonitoringDoc.id("_state_uuid_3", shardRouting));
    }

    @Override
    public void testToXContent() throws IOException {
        final ShardRouting shardRouting = newShardRouting("_index", 1, "_index_uuid", "_node_uuid", true, INITIALIZING);
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final ShardMonitoringDoc doc =
                new ShardMonitoringDoc("_cluster", 1502107402133L, 1506593717631L, node, shardRouting, "_state_uuid");

        final BytesReference xContent = XContentHelper.toXContent(doc, XContentType.JSON, randomBoolean());
        final String expected = "{"
            + "  \"cluster_uuid\": \"_cluster\","
            + "  \"timestamp\": \"2017-08-07T12:03:22.133Z\","
            + "  \"interval_ms\": 1506593717631,"
            + "  \"type\": \"shards\","
            + "  \"source_node\": {"
            + "    \"uuid\": \"_uuid\","
            + "    \"host\": \"_host\","
            + "    \"transport_address\": \"_addr\","
            + "    \"ip\": \"_ip\","
            + "    \"name\": \"_name\","
            + "    \"timestamp\": \"2017-08-31T08:46:30.855Z\""
            + "  },"
            + "  \"state_uuid\": \"_state_uuid\","
            + "  \"shard\": {"
            + "    \"state\": \"INITIALIZING\","
            + "    \"primary\": true,"
            + "    \"node\": \"_index_uuid\","
            + "    \"relocating_node\": \"_node_uuid\","
            + "    \"shard\": 1,"
            + "    \"index\": \"_index\""
            + "  }"
            + "}";
        assertEquals(XContentHelper.stripWhitespace(expected), xContent.utf8ToString());
    }
}
