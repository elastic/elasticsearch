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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringTemplateRegistry;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AutoFollowStatsMonitoringDocTests extends BaseMonitoringDocTestCase<AutoFollowStatsMonitoringDoc> {
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    private AutoFollowStats autoFollowStats;

    @Before
    public void instantiateAutoFollowStats() {
        autoFollowStats = new AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Collections.emptyNavigableMap(),
            Collections.emptyNavigableMap()
        );
    }

    @Override
    protected AutoFollowStatsMonitoringDoc createMonitoringDoc(
        String cluster,
        long timestamp,
        long interval,
        MonitoringDoc.Node node,
        MonitoredSystem system,
        String type,
        String id
    ) {
        return new AutoFollowStatsMonitoringDoc(cluster, timestamp, interval, node, autoFollowStats);
    }

    @Override
    protected void assertMonitoringDoc(AutoFollowStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(AutoFollowStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.stats(), is(autoFollowStats));
    }

    @Override
    public void testToXContent() throws IOException {
        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = System.currentTimeMillis();
        final long nodeTimestamp = System.currentTimeMillis();
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", nodeTimestamp);

        final NavigableMap<String, Tuple<Long, ElasticsearchException>> recentAutoFollowExceptions = new TreeMap<>(
            Collections.singletonMap(randomAlphaOfLength(4), Tuple.tuple(1L, new ElasticsearchException("cannot follow index")))
        );

        final NavigableMap<String, AutoFollowedCluster> trackingClusters = new TreeMap<>(
            Collections.singletonMap(randomAlphaOfLength(4), new AutoFollowedCluster(1L, 1L))
        );
        autoFollowStats = new AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            recentAutoFollowExceptions,
            trackingClusters
        );

        final AutoFollowStatsMonitoringDoc document = new AutoFollowStatsMonitoringDoc(
            "_cluster",
            timestamp,
            intervalMillis,
            node,
            autoFollowStats
        );
        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertThat(
            xContent.utf8ToString(),
            equalTo(
                XContentHelper.stripWhitespace(
                    """
                        {
                          "cluster_uuid": "_cluster",
                          "timestamp": "%s",
                          "interval_ms": %s,
                          "type": "ccr_auto_follow_stats",
                          "source_node": {
                            "uuid": "_uuid",
                            "host": "_host",
                            "transport_address": "_addr",
                            "ip": "_ip",
                            "name": "_name",
                            "timestamp": "%s"
                          },
                          "ccr_auto_follow_stats": {
                            "number_of_failed_follow_indices": %s,
                            "number_of_failed_remote_cluster_state_requests": %s,
                            "number_of_successful_follow_indices": %s,
                            "recent_auto_follow_errors": [
                              {
                                "leader_index": "%s",
                                "timestamp": 1,
                                "auto_follow_exception": {
                                  "type": "exception",
                                  "reason": "cannot follow index"
                                }
                              }
                            ],
                            "auto_followed_clusters": [
                              {
                                "cluster_name": "%s",
                                "time_since_last_check_millis": %s,
                                "last_seen_metadata_version": %s
                              }
                            ]
                          }
                        }""".formatted(
                        DATE_TIME_FORMATTER.formatMillis(timestamp),
                        intervalMillis,
                        DATE_TIME_FORMATTER.formatMillis(nodeTimestamp),
                        autoFollowStats.getNumberOfFailedFollowIndices(),
                        autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(),
                        autoFollowStats.getNumberOfSuccessfulFollowIndices(),
                        recentAutoFollowExceptions.keySet().iterator().next(),
                        trackingClusters.keySet().iterator().next(),
                        trackingClusters.values().iterator().next().getTimeSinceLastCheckMillis(),
                        trackingClusters.values().iterator().next().getLastSeenMetadataVersion()
                    )
                )
            )
        );
    }

    public void testShardFollowNodeTaskStatusFieldsMapped() throws IOException {
        final NavigableMap<String, Tuple<Long, ElasticsearchException>> fetchExceptions = new TreeMap<>(
            Collections.singletonMap("leader_index", Tuple.tuple(1L, new ElasticsearchException("cannot follow index")))
        );
        final NavigableMap<String, AutoFollowedCluster> trackingClusters = new TreeMap<>(
            Collections.singletonMap(randomAlphaOfLength(4), new AutoFollowedCluster(1L, 1L))
        );
        final AutoFollowStats status = new AutoFollowStats(1, 0, 2, fetchExceptions, trackingClusters);
        XContentBuilder builder = jsonBuilder();
        builder.value(status);
        Map<String, Object> serializedStatus = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false);

        byte[] loadedTemplate = MonitoringTemplateRegistry.getTemplateConfigForMonitoredSystem(MonitoredSystem.ES).loadBytes();
        Map<String, Object> template = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            loadedTemplate,
            0,
            loadedTemplate.length,
            false
        );
        Map<?, ?> autoFollowStatsMapping = (Map<?, ?>) XContentMapValues.extractValue(
            "mappings._doc.properties.ccr_auto_follow_stats.properties",
            template
        );

        assertThat(serializedStatus.size(), equalTo(autoFollowStatsMapping.size()));
        for (Map.Entry<String, Object> entry : serializedStatus.entrySet()) {
            String fieldName = entry.getKey();
            Map<?, ?> fieldMapping = (Map<?, ?>) autoFollowStatsMapping.get(fieldName);
            assertThat(fieldMapping, notNullValue());

            Object fieldValue = entry.getValue();
            String fieldType = (String) fieldMapping.get("type");
            if (fieldValue instanceof Long || fieldValue instanceof Integer) {
                assertThat("expected long field type for field [" + fieldName + "]", fieldType, anyOf(equalTo("long"), equalTo("integer")));
            } else if (fieldValue instanceof String) {
                assertThat(
                    "expected keyword field type for field [" + fieldName + "]",
                    fieldType,
                    anyOf(equalTo("keyword"), equalTo("text"))
                );
            } else {
                Map<?, ?> innerFieldValue = (Map<?, ?>) ((List) fieldValue).get(0);
                // Manual test specific object fields and if not just fail:
                if (fieldName.equals("recent_auto_follow_errors")) {
                    assertThat(fieldType, equalTo("nested"));
                    assertThat(((Map<?, ?>) fieldMapping.get("properties")).size(), equalTo(innerFieldValue.size()));
                    assertThat(XContentMapValues.extractValue("properties.leader_index.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.timestamp.type", fieldMapping), equalTo("long"));
                    assertThat(XContentMapValues.extractValue("properties.auto_follow_exception.type", fieldMapping), equalTo("object"));

                    innerFieldValue = (Map<?, ?>) innerFieldValue.get("auto_follow_exception");
                    Map<?, ?> exceptionFieldMapping = (Map<?, ?>) XContentMapValues.extractValue(
                        "properties.auto_follow_exception.properties",
                        fieldMapping
                    );
                    assertThat(exceptionFieldMapping.size(), equalTo(innerFieldValue.size()));
                    assertThat(XContentMapValues.extractValue("type.type", exceptionFieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("reason.type", exceptionFieldMapping), equalTo("text"));
                } else if (fieldName.equals("auto_followed_clusters")) {
                    assertThat(fieldType, equalTo("nested"));
                    Map<?, ?> innerFieldMapping = ((Map<?, ?>) fieldMapping.get("properties"));
                    assertThat(innerFieldMapping.size(), equalTo(innerFieldValue.size()));

                    assertThat(XContentMapValues.extractValue("cluster_name.type", innerFieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("time_since_last_check_millis.type", innerFieldMapping), equalTo("long"));
                    assertThat(XContentMapValues.extractValue("last_seen_metadata_version.type", innerFieldMapping), equalTo("long"));
                } else {
                    fail("unexpected field value type [" + fieldValue.getClass() + "] for field [" + fieldName + "]");
                }
            }
        }
    }
}
