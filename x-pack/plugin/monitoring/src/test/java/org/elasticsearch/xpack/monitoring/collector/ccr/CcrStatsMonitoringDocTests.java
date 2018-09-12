/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class CcrStatsMonitoringDocTests extends BaseMonitoringDocTestCase<CcrStatsMonitoringDoc> {

    private ShardFollowNodeTaskStatus status;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        status = mock(ShardFollowNodeTaskStatus.class);
    }

    public void testConstructorStatusMustNotBeNull() {
        final NullPointerException e =
                expectThrows(NullPointerException.class, () -> new CcrStatsMonitoringDoc(cluster, timestamp, interval, node, null));
        assertThat(e, hasToString(containsString("status")));
    }

    @Override
    protected CcrStatsMonitoringDoc createMonitoringDoc(
            final String cluster,
            final long timestamp,
            final long interval,
            final MonitoringDoc.Node node,
            final MonitoredSystem system,
            final String type,
            final String id) {
        return new CcrStatsMonitoringDoc(cluster, timestamp, interval, node, status);
    }

    @Override
    protected void assertMonitoringDoc(CcrStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(CcrStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.status(), is(status));
    }

    @Override
    public void testToXContent() throws IOException {
        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = System.currentTimeMillis();
        final long nodeTimestamp = System.currentTimeMillis();
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", nodeTimestamp);
        // these random values do not need to be internally consistent, they are only for testing formatting
        final int shardId = randomIntBetween(0, Integer.MAX_VALUE);
        final long leaderGlobalCheckpoint = randomNonNegativeLong();
        final long leaderMaxSeqNo = randomNonNegativeLong();
        final long followerGlobalCheckpoint = randomNonNegativeLong();
        final long followerMaxSeqNo = randomNonNegativeLong();
        final long lastRequestedSeqNo = randomNonNegativeLong();
        final int numberOfConcurrentReads = randomIntBetween(1, Integer.MAX_VALUE);
        final int numberOfConcurrentWrites = randomIntBetween(1, Integer.MAX_VALUE);
        final int numberOfQueuedWrites = randomIntBetween(0, Integer.MAX_VALUE);
        final long mappingVersion = randomIntBetween(0, Integer.MAX_VALUE);
        final long totalFetchTimeMillis = randomLongBetween(0, 4096);
        final long numberOfSuccessfulFetches = randomNonNegativeLong();
        final long numberOfFailedFetches = randomLongBetween(0, 8);
        final long operationsReceived = randomNonNegativeLong();
        final long totalTransferredBytes = randomNonNegativeLong();
        final long totalIndexTimeMillis = randomNonNegativeLong();
        final long numberOfSuccessfulBulkOperations = randomNonNegativeLong();
        final long numberOfFailedBulkOperations = randomNonNegativeLong();
        final long numberOfOperationsIndexed = randomNonNegativeLong();
        final NavigableMap<Long, ElasticsearchException> fetchExceptions =
                new TreeMap<>(Collections.singletonMap(randomNonNegativeLong(), new ElasticsearchException("shard is sad")));
        final long timeSinceLastFetchMillis = randomNonNegativeLong();
        final ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
                "cluster_alias:leader_index",
                shardId,
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                numberOfConcurrentReads,
                numberOfConcurrentWrites,
                numberOfQueuedWrites,
                mappingVersion,
                totalFetchTimeMillis,
                numberOfSuccessfulFetches,
                numberOfFailedFetches,
                operationsReceived,
                totalTransferredBytes,
                totalIndexTimeMillis,
                numberOfSuccessfulBulkOperations,
                numberOfFailedBulkOperations,
                numberOfOperationsIndexed,
                fetchExceptions,
                timeSinceLastFetchMillis);
        final CcrStatsMonitoringDoc document = new CcrStatsMonitoringDoc("_cluster", timestamp, intervalMillis, node, status);
        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertThat(
                xContent.utf8ToString(),
                equalTo(
                        "{"
                                + "\"cluster_uuid\":\"_cluster\","
                                + "\"timestamp\":\"" + new DateTime(timestamp, DateTimeZone.UTC).toString() + "\","
                                + "\"interval_ms\":" + intervalMillis + ","
                                + "\"type\":\"ccr_stats\","
                                + "\"source_node\":{"
                                        + "\"uuid\":\"_uuid\","
                                        + "\"host\":\"_host\","
                                        + "\"transport_address\":\"_addr\","
                                        + "\"ip\":\"_ip\","
                                        + "\"name\":\"_name\","
                                        + "\"timestamp\":\"" + new DateTime(nodeTimestamp, DateTimeZone.UTC).toString() +  "\""
                                + "},"
                                + "\"ccr_stats\":{"
                                        + "\"leader_index\":\"cluster_alias:leader_index\","
                                        + "\"shard_id\":" + shardId + ","
                                        + "\"leader_global_checkpoint\":" + leaderGlobalCheckpoint + ","
                                        + "\"leader_max_seq_no\":" + leaderMaxSeqNo + ","
                                        + "\"follower_global_checkpoint\":" + followerGlobalCheckpoint + ","
                                        + "\"follower_max_seq_no\":" + followerMaxSeqNo + ","
                                        + "\"last_requested_seq_no\":" + lastRequestedSeqNo + ","
                                        + "\"number_of_concurrent_reads\":" + numberOfConcurrentReads + ","
                                        + "\"number_of_concurrent_writes\":" + numberOfConcurrentWrites + ","
                                        + "\"number_of_queued_writes\":" + numberOfQueuedWrites + ","
                                        + "\"mapping_version\":" + mappingVersion + ","
                                        + "\"total_fetch_time_millis\":" + totalFetchTimeMillis + ","
                                        + "\"number_of_successful_fetches\":" + numberOfSuccessfulFetches + ","
                                        + "\"number_of_failed_fetches\":" + numberOfFailedFetches + ","
                                        + "\"operations_received\":" + operationsReceived + ","
                                        + "\"total_transferred_bytes\":" + totalTransferredBytes + ","
                                        + "\"total_index_time_millis\":" + totalIndexTimeMillis +","
                                        + "\"number_of_successful_bulk_operations\":" + numberOfSuccessfulBulkOperations + ","
                                        + "\"number_of_failed_bulk_operations\":" + numberOfFailedBulkOperations + ","
                                        + "\"number_of_operations_indexed\":" + numberOfOperationsIndexed + ","
                                        + "\"fetch_exceptions\":["
                                                + "{"
                                                        + "\"from_seq_no\":" + fetchExceptions.keySet().iterator().next() + ","
                                                        + "\"exception\":{"
                                                                + "\"type\":\"exception\","
                                                                + "\"reason\":\"shard is sad\""
                                                        + "}"
                                                + "}"
                                        + "],"
                                        + "\"time_since_last_fetch_millis\":" + timeSinceLastFetchMillis
                                + "}"
                        + "}"));
    }

}
