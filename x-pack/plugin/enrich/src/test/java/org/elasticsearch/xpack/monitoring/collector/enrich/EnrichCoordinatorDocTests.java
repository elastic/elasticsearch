/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EnrichCoordinatorDocTests extends BaseMonitoringDocTestCase<EnrichCoordinatorDoc> {

    static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);

    private CoordinatorStats stats;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        stats = new CoordinatorStats(
            randomAlphaOfLength(4),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected EnrichCoordinatorDoc createMonitoringDoc(
        String cluster,
        long timestamp,
        long interval,
        MonitoringDoc.Node node,
        MonitoredSystem system,
        String type,
        String id
    ) {

        return new EnrichCoordinatorDoc(cluster, timestamp, interval, node, stats);
    }

    @Override
    protected void assertMonitoringDoc(EnrichCoordinatorDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(EnrichCoordinatorDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.getCoordinatorStats(), equalTo(stats));
    }

    @Override
    public void testToXContent() throws IOException {
        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = System.currentTimeMillis();
        final long nodeTimestamp = System.currentTimeMillis();
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", nodeTimestamp);

        final EnrichCoordinatorDoc document = new EnrichCoordinatorDoc("_cluster", timestamp, intervalMillis, node, stats);
        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertThat(
            xContent.utf8ToString(),
            equalTo(
                "{"
                    + "\"cluster_uuid\":\"_cluster\","
                    + "\"timestamp\":\""
                    + DATE_TIME_FORMATTER.formatMillis(timestamp)
                    + "\","
                    + "\"interval_ms\":"
                    + intervalMillis
                    + ","
                    + "\"type\":\"enrich_coordinator_stats\","
                    + "\"source_node\":{"
                    + "\"uuid\":\"_uuid\","
                    + "\"host\":\"_host\","
                    + "\"transport_address\":\"_addr\","
                    + "\"ip\":\"_ip\","
                    + "\"name\":\"_name\","
                    + "\"timestamp\":\""
                    + DATE_TIME_FORMATTER.formatMillis(nodeTimestamp)
                    + "\""
                    + "},"
                    + "\"enrich_coordinator_stats\":{"
                    + "\"node_id\":\""
                    + stats.getNodeId()
                    + "\","
                    + "\"queue_size\":"
                    + stats.getQueueSize()
                    + ","
                    + "\"remote_requests_current\":"
                    + stats.getRemoteRequestsCurrent()
                    + ","
                    + "\"remote_requests_total\":"
                    + stats.getRemoteRequestsTotal()
                    + ","
                    + "\"executed_searches_total\":"
                    + stats.getExecutedSearchesTotal()
                    + "}"
                    + "}"
            )
        );
    }

    public void testEnrichCoordinatorStatsFieldsMapped() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.value(stats);
        builder.endObject();
        Map<String, Object> serializedStatus = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false);

        Map<String, Object> template = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            MonitoringTemplateUtils.loadTemplate("es"),
            false
        );
        Map<?, ?> followStatsMapping = (Map<?, ?>) XContentMapValues.extractValue(
            "mappings._doc.properties.enrich_coordinator_stats.properties",
            template
        );
        assertThat(serializedStatus.size(), equalTo(followStatsMapping.size()));
        for (Map.Entry<String, Object> entry : serializedStatus.entrySet()) {
            String fieldName = entry.getKey();
            Map<?, ?> fieldMapping = (Map<?, ?>) followStatsMapping.get(fieldName);
            assertThat("no field mapping for field [" + fieldName + "]", fieldMapping, notNullValue());

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
                // Manual test specific object fields and if not just fail:
                fail("unexpected field value type [" + fieldValue.getClass() + "] for field [" + fieldName + "]");
            }
        }
    }
}
