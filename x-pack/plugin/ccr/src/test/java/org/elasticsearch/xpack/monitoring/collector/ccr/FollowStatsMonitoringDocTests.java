/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class FollowStatsMonitoringDocTests extends BaseMonitoringDocTestCase<FollowStatsMonitoringDoc> {
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    private ShardFollowNodeTaskStatus status;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        status = mock(ShardFollowNodeTaskStatus.class);
    }

    public void testConstructorStatusMustNotBeNull() {
        final NullPointerException e =
                expectThrows(NullPointerException.class, () -> new FollowStatsMonitoringDoc(cluster, timestamp, interval, node, null));
        assertThat(e, hasToString(containsString("status")));
    }

    @Override
    protected FollowStatsMonitoringDoc createMonitoringDoc(
            final String cluster,
            final long timestamp,
            final long interval,
            final MonitoringDoc.Node node,
            final MonitoredSystem system,
            final String type,
            final String id) {
        return new FollowStatsMonitoringDoc(cluster, timestamp, interval, node, status);
    }

    @Override
    protected void assertMonitoringDoc(FollowStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(FollowStatsMonitoringDoc.TYPE));
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
        final int writeBufferOperationCount = randomIntBetween(0, Integer.MAX_VALUE);
        final long writeBufferSizeInBytes = randomNonNegativeLong();
        final long followerMappingVersion = randomNonNegativeLong();
        final long followerSettingsVersion = randomNonNegativeLong();
        final long followerAliasesVersion = randomNonNegativeLong();
        final long totalReadTimeMillis = randomLongBetween(0, 4096);
        final long totalReadRemoteExecTimeMillis = randomLongBetween(0, 4096);
        final long successfulReadRequests = randomNonNegativeLong();
        final long failedReadRequests = randomLongBetween(0, 8);
        final long operationsRead = randomNonNegativeLong();
        final long bytesRead = randomNonNegativeLong();
        final long totalWriteTimeMillis = randomNonNegativeLong();
        final long successfulWriteRequests = randomNonNegativeLong();
        final long failedWriteRequests = randomNonNegativeLong();
        final long operationWritten = randomNonNegativeLong();
        final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> fetchExceptions =
                new TreeMap<>(Collections.singletonMap(
                        randomNonNegativeLong(),
                        Tuple.tuple(randomIntBetween(0, Integer.MAX_VALUE), new ElasticsearchException("shard is sad"))));
        final long timeSinceLastReadMillis = randomNonNegativeLong();
        final ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
                "leader_cluster",
                "leader_index",
                "follower_index",
                shardId,
                leaderGlobalCheckpoint,
                leaderMaxSeqNo,
                followerGlobalCheckpoint,
                followerMaxSeqNo,
                lastRequestedSeqNo,
                numberOfConcurrentReads,
                numberOfConcurrentWrites,
                writeBufferOperationCount,
                writeBufferSizeInBytes,
                followerMappingVersion,
                followerSettingsVersion,
                followerAliasesVersion,
                totalReadTimeMillis,
                totalReadRemoteExecTimeMillis,
                successfulReadRequests,
                failedReadRequests,
                operationsRead,
                bytesRead,
                totalWriteTimeMillis,
                successfulWriteRequests,
                failedWriteRequests,
                operationWritten,
                fetchExceptions,
                timeSinceLastReadMillis,
                new ElasticsearchException("fatal error"));
        final FollowStatsMonitoringDoc document = new FollowStatsMonitoringDoc("_cluster", timestamp, intervalMillis, node, status);
        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertThat(
                xContent.utf8ToString(),
                equalTo(
                        "{"
                                + "\"cluster_uuid\":\"_cluster\","
                                + "\"timestamp\":\"" + DATE_TIME_FORMATTER.formatMillis(timestamp) + "\","
                                + "\"interval_ms\":" + intervalMillis + ","
                                + "\"type\":\"ccr_stats\","
                                + "\"source_node\":{"
                                        + "\"uuid\":\"_uuid\","
                                        + "\"host\":\"_host\","
                                        + "\"transport_address\":\"_addr\","
                                        + "\"ip\":\"_ip\","
                                        + "\"name\":\"_name\","
                                        + "\"timestamp\":\"" + DATE_TIME_FORMATTER.formatMillis(nodeTimestamp) +  "\""
                                + "},"
                                + "\"ccr_stats\":{"
                                        + "\"remote_cluster\":\"leader_cluster\","
                                        + "\"leader_index\":\"leader_index\","
                                        + "\"follower_index\":\"follower_index\","
                                        + "\"shard_id\":" + shardId + ","
                                        + "\"leader_global_checkpoint\":" + leaderGlobalCheckpoint + ","
                                        + "\"leader_max_seq_no\":" + leaderMaxSeqNo + ","
                                        + "\"follower_global_checkpoint\":" + followerGlobalCheckpoint + ","
                                        + "\"follower_max_seq_no\":" + followerMaxSeqNo + ","
                                        + "\"last_requested_seq_no\":" + lastRequestedSeqNo + ","
                                        + "\"outstanding_read_requests\":" + numberOfConcurrentReads + ","
                                        + "\"outstanding_write_requests\":" + numberOfConcurrentWrites + ","
                                        + "\"write_buffer_operation_count\":" + writeBufferOperationCount + ","
                                        + "\"write_buffer_size_in_bytes\":" + writeBufferSizeInBytes + ","
                                        + "\"follower_mapping_version\":" + followerMappingVersion + ","
                                        + "\"follower_settings_version\":" + followerSettingsVersion + ","
                                        + "\"follower_aliases_version\":" + followerAliasesVersion + ","
                                        + "\"total_read_time_millis\":" + totalReadTimeMillis + ","
                                        + "\"total_read_remote_exec_time_millis\":" + totalReadRemoteExecTimeMillis + ","
                                        + "\"successful_read_requests\":" + successfulReadRequests + ","
                                        + "\"failed_read_requests\":" + failedReadRequests + ","
                                        + "\"operations_read\":" + operationsRead + ","
                                        + "\"bytes_read\":" + bytesRead + ","
                                        + "\"total_write_time_millis\":" + totalWriteTimeMillis +","
                                        + "\"successful_write_requests\":" + successfulWriteRequests + ","
                                        + "\"failed_write_requests\":" + failedWriteRequests + ","
                                        + "\"operations_written\":" + operationWritten + ","
                                        + "\"read_exceptions\":["
                                                + "{"
                                                        + "\"from_seq_no\":" + fetchExceptions.keySet().iterator().next() + ","
                                                        + "\"retries\":" + fetchExceptions.values().iterator().next().v1() + ","
                                                        + "\"exception\":{"
                                                                + "\"type\":\"exception\","
                                                                + "\"reason\":\"shard is sad\""
                                                        + "}"
                                                + "}"
                                        + "],"
                                        + "\"time_since_last_read_millis\":" + timeSinceLastReadMillis + ","
                                        + "\"fatal_exception\":{\"type\":\"exception\",\"reason\":\"fatal error\"}"
                                + "}"
                        + "}"));
    }

    public void testShardFollowNodeTaskStatusFieldsMapped() throws IOException {
        final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> fetchExceptions =
            new TreeMap<>(Collections.singletonMap(1L, Tuple.tuple(2, new ElasticsearchException("shard is sad"))));
        final ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
            "remote_cluster",
            "leader_index",
            "follower_index",
            0,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            100,
            50,
            10,
            0,
            10,
            100,
            10,
            10,
            0,
            10,
            fetchExceptions,
            2,
            new ElasticsearchException("fatal error"));
        XContentBuilder builder = jsonBuilder();
        builder.value(status);
        Map<String, Object> serializedStatus = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false);

        Map<String, Object> template =
            XContentHelper.convertToMap(XContentType.JSON.xContent(), MonitoringTemplateUtils.loadTemplate("es"), false);
        Map<?, ?> followStatsMapping = (Map<?, ?>) XContentMapValues
            .extractValue("mappings._doc.properties.ccr_stats.properties", template);
        assertThat(serializedStatus.size(), equalTo(followStatsMapping.size()));
        for (Map.Entry<String, Object> entry : serializedStatus.entrySet()) {
            String fieldName = entry.getKey();
            Map<?, ?> fieldMapping = (Map<?, ?>) followStatsMapping.get(fieldName);
            assertThat("no field mapping for field [" + fieldName + "]", fieldMapping, notNullValue());

            Object fieldValue = entry.getValue();
            String fieldType = (String) fieldMapping.get("type");
            if (fieldValue instanceof Long || fieldValue instanceof Integer) {
                assertThat("expected long field type for field [" + fieldName + "]", fieldType,
                    anyOf(equalTo("long"), equalTo("integer")));
            } else if (fieldValue instanceof String) {
                assertThat("expected keyword field type for field [" + fieldName + "]", fieldType,
                    anyOf(equalTo("keyword"), equalTo("text")));
            } else {
                // Manual test specific object fields and if not just fail:
                if (fieldName.equals("read_exceptions")) {
                    assertThat(fieldType, equalTo("nested"));
                    assertThat(((Map<?, ?>) fieldMapping.get("properties")).size(), equalTo(3));
                    assertThat(XContentMapValues.extractValue("properties.from_seq_no.type", fieldMapping), equalTo("long"));
                    assertThat(XContentMapValues.extractValue("properties.retries.type", fieldMapping), equalTo("integer"));
                    assertThat(XContentMapValues.extractValue("properties.exception.type", fieldMapping), equalTo("object"));

                    Map<?, ?> exceptionFieldMapping =
                        (Map<?, ?>) XContentMapValues.extractValue("properties.exception.properties", fieldMapping);
                    assertThat(exceptionFieldMapping.size(), equalTo(2));
                    assertThat(XContentMapValues.extractValue("type.type", exceptionFieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("reason.type", exceptionFieldMapping), equalTo("text"));
                } else if (fieldName.equals("fatal_exception")) {
                    assertThat(fieldType, equalTo("object"));
                    assertThat(((Map<?, ?>) fieldMapping.get("properties")).size(), equalTo(2));
                    assertThat(XContentMapValues.extractValue("properties.type.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.reason.type", fieldMapping), equalTo("text"));
                } else {
                    fail("unexpected field value type [" + fieldValue.getClass() + "] for field [" + fieldName + "]");
                }
            }
        }
    }

}
