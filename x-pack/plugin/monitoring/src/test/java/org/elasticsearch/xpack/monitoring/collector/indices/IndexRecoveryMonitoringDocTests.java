/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class IndexRecoveryMonitoringDocTests extends BaseMonitoringDocTestCase<IndexRecoveryMonitoringDoc> {

    private RecoveryResponse recoveryResponse;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        recoveryResponse = mock(RecoveryResponse.class);
    }

    @Override
    protected IndexRecoveryMonitoringDoc createMonitoringDoc(String cluster, long timestamp, long interval, MonitoringDoc.Node node,
                                                             MonitoredSystem system, String type, String id) {
        return new IndexRecoveryMonitoringDoc(cluster, timestamp, interval, node, recoveryResponse);
    }

    @Override
    protected void assertMonitoringDoc(final IndexRecoveryMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(IndexRecoveryMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getRecoveryResponse(), is(recoveryResponse));
    }

    public void testConstructorRecoveryResponseMustNotBeNull() {
        expectThrows(NullPointerException.class,
                () -> new IndexRecoveryMonitoringDoc(cluster, timestamp, interval, node, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final DiscoveryNode discoveryNodeZero = new DiscoveryNode("_node_0",
                                                                    "_node_id_0",
                                                                    "_ephemeral_id_0",
                                                                    "_host_name_0",
                                                                    "_host_address_0",
                                                                    new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                                                                    singletonMap("attr", "value_0"),
                                                                    singleton(DiscoveryNodeRole.MASTER_ROLE),
                                                                    Version.CURRENT);

        final DiscoveryNode discoveryNodeOne = new DiscoveryNode("_node_1",
                                                                    "_node_id_1",
                                                                    "_ephemeral_id_1",
                                                                    "_host_name_1",
                                                                    "_host_address_1",
                                                                    new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                                                                    singletonMap("attr", "value_1"),
                                                                    singleton(DiscoveryNodeRole.DATA_ROLE),
                                                                    Version.CURRENT.minimumIndexCompatibilityVersion());

        final ShardId shardId = new ShardId("_index_a", "_uuid_a", 0);
        final RecoverySource source = RecoverySource.PeerRecoverySource.INSTANCE;
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_index_info_a");
        final ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, source, unassignedInfo)
                                                      .initialize("_node_id", "_allocation_id", 123L);

        final Map<String, List<RecoveryState>> shardRecoveryStates = new HashMap<>();
        final RecoveryState recoveryState = new RecoveryState(shardRouting, discoveryNodeOne, discoveryNodeOne);
        shardRecoveryStates.put("_shard_0", singletonList(recoveryState));

        final RecoveryState.Timer timer = recoveryState.getTimer();
        timer.stop();

        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final Throwable reason = new NodeDisconnectedException(discoveryNodeZero, "");
        shardFailures.add(new IndicesShardStoresResponse.Failure("_failed_node_id", "_failed_index", 1, reason));

        final RecoveryResponse recoveryResponse = new RecoveryResponse(10, 7, 3, shardRecoveryStates, shardFailures);
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final IndexRecoveryMonitoringDoc document =
                new IndexRecoveryMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, recoveryResponse);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        final String expected = XContentHelper.stripWhitespace(
            String.format(
                Locale.ROOT,
                "{"
                    + "  \"cluster_uuid\": \"_cluster\","
                    + "  \"timestamp\": \"2017-08-09T08:18:59.402Z\","
                    + "  \"interval_ms\": 1506593717631,"
                    + "  \"type\": \"index_recovery\","
                    + "  \"source_node\": {"
                    + "    \"uuid\": \"_uuid\","
                    + "    \"host\": \"_host\","
                    + "    \"transport_address\": \"_addr\","
                    + "    \"ip\": \"_ip\","
                    + "    \"name\": \"_name\","
                    + "    \"timestamp\": \"2017-08-31T08:46:30.855Z\""
                    + "  },"
                    + "  \"index_recovery\": {"
                    + "    \"shards\": ["
                    + "      {"
                    + "        \"index_name\": \"_shard_0\","
                    + "        \"id\": 0,"
                    + "        \"type\": \"PEER\","
                    + "        \"stage\": \"INIT\","
                    + "        \"primary\": true,"
                    + "        \"start_time_in_millis\": %d,"
                    + "        \"stop_time_in_millis\": %d,"
                    + "        \"total_time_in_millis\": %d,"
                    + "        \"source\": {"
                    + "          \"id\": \"_node_id_1\","
                    + "          \"host\": \"_host_name_1\","
                    + "          \"transport_address\": \"0.0.0.0:9301\","
                    + "          \"ip\": \"_host_address_1\","
                    + "          \"name\": \"_node_1\""
                    + "        },"
                    + "        \"target\": {"
                    + "          \"id\": \"_node_id_1\","
                    + "          \"host\": \"_host_name_1\","
                    + "          \"transport_address\": \"0.0.0.0:9301\","
                    + "          \"ip\": \"_host_address_1\","
                    + "          \"name\": \"_node_1\""
                    + "        },"
                    + "        \"index\": {"
                    + "          \"size\": {"
                    + "            \"total_in_bytes\": 0,"
                    + "            \"reused_in_bytes\": 0,"
                    + "            \"recovered_in_bytes\": 0,"
                    + "            \"recovered_from_snapshot_in_bytes\": 0,"
                    + "            \"percent\": \"0.0%%\""
                    + "          },"
                    + "          \"files\": {"
                    + "            \"total\": 0,"
                    + "            \"reused\": 0,"
                    + "            \"recovered\": 0,"
                    + "            \"percent\": \"0.0%%\""
                    + "          },"
                    + "          \"total_time_in_millis\": 0,"
                    + "          \"source_throttle_time_in_millis\": 0,"
                    + "          \"target_throttle_time_in_millis\": 0"
                    + "        },"
                    + "        \"translog\": {"
                    + "          \"recovered\": 0,"
                    + "          \"total\": -1,"
                    + "          \"percent\": \"-1.0%%\","
                    + "          \"total_on_start\": -1,"
                    + "          \"total_time_in_millis\": 0"
                    + "        },"
                    + "        \"verify_index\": {"
                    + "          \"check_index_time_in_millis\": 0,"
                    + "          \"total_time_in_millis\": 0"
                    + "        }"
                    + "      }"
                    + "    ]"
                    + "  }"
                    + "}",
                timer.startTime(),
                timer.stopTime(),
                timer.time()
            )
        );
        assertEquals(expected, xContent.utf8ToString());
    }
}
