/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringTemplateRegistry;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.enrich.action.EnrichStatsResponseTests.randomTaskInfo;
import static org.elasticsearch.xpack.monitoring.collector.enrich.EnrichCoordinatorDocTests.DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ExecutingPolicyDocTests extends BaseMonitoringDocTestCase<ExecutingPolicyDoc> {

    private ExecutingPolicy executingPolicy;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executingPolicy = new ExecutingPolicy(randomAlphaOfLength(4), randomTaskInfo());
    }

    @Override
    protected ExecutingPolicyDoc createMonitoringDoc(
        String cluster,
        long timestamp,
        long interval,
        MonitoringDoc.Node node,
        MonitoredSystem system,
        String type,
        String id
    ) {

        return new ExecutingPolicyDoc(cluster, timestamp, interval, node, executingPolicy);
    }

    @Override
    protected void assertMonitoringDoc(ExecutingPolicyDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(ExecutingPolicyDoc.TYPE));
        assertThat(document.getId(), nullValue());
        assertThat(document.getExecutingPolicy(), equalTo(executingPolicy));
    }

    @Override
    public void testToXContent() throws IOException {
        final long timestamp = System.currentTimeMillis();
        final long intervalMillis = System.currentTimeMillis();
        final long nodeTimestamp = System.currentTimeMillis();
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", nodeTimestamp);

        final ExecutingPolicyDoc document = new ExecutingPolicyDoc("_cluster", timestamp, intervalMillis, node, executingPolicy);
        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        Optional<Map.Entry<String, String>> header = executingPolicy.getTaskInfo().headers().entrySet().stream().findAny();
        assertThat(
            xContent.utf8ToString(),
            equalTo(
                XContentHelper.stripWhitespace(
                    """
                        {
                          "cluster_uuid": "_cluster",
                          "timestamp": "%s",
                          "interval_ms": %s,
                          "type": "enrich_executing_policy_stats",
                          "source_node": {
                            "uuid": "_uuid",
                            "host": "_host",
                            "transport_address": "_addr",
                            "ip": "_ip",
                            "name": "_name",
                            "timestamp": "%s"
                          },
                          "enrich_executing_policy_stats": {
                            "name": "%s",
                            "task": {
                              "node": "%s",
                              "id": %s,
                              "type": "%s",
                              "action": "%s",
                              "description": "%s",
                              "start_time_in_millis": %s,
                              "running_time_in_nanos": %s,
                              "cancellable": %s,
                              %s
                              "headers": %s
                            }
                          }
                        }""".formatted(
                        DATE_TIME_FORMATTER.formatMillis(timestamp),
                        intervalMillis,
                        DATE_TIME_FORMATTER.formatMillis(nodeTimestamp),
                        executingPolicy.getName(),
                        executingPolicy.getTaskInfo().taskId().getNodeId(),
                        executingPolicy.getTaskInfo().taskId().getId(),
                        executingPolicy.getTaskInfo().type(),
                        executingPolicy.getTaskInfo().action(),
                        executingPolicy.getTaskInfo().description(),
                        executingPolicy.getTaskInfo().startTime(),
                        executingPolicy.getTaskInfo().runningTimeNanos(),
                        executingPolicy.getTaskInfo().cancellable(),
                        executingPolicy.getTaskInfo().cancellable()
                            ? "\"cancelled\": %s,".formatted(executingPolicy.getTaskInfo().cancelled())
                            : "",
                        header.map(entry -> String.format(Locale.ROOT, """
                            {"%s":"%s"}""", entry.getKey(), entry.getValue())).orElse("{}")
                    )
                )
            )
        );
    }

    public void testEnrichCoordinatorStatsFieldsMapped() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.value(executingPolicy);
        builder.endObject();
        Map<String, Object> serializedStatus = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false);

        byte[] loadedTemplate = MonitoringTemplateRegistry.getTemplateConfigForMonitoredSystem(MonitoredSystem.ES).loadBytes();
        Map<String, Object> template = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            loadedTemplate,
            0,
            loadedTemplate.length,
            false
        );
        Map<?, ?> followStatsMapping = (Map<?, ?>) XContentMapValues.extractValue(
            "mappings._doc.properties.enrich_executing_policy_stats.properties",
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
                if (fieldName.equals("task")) {
                    assertThat(fieldType, equalTo("object"));
                    assertThat(((Map<?, ?>) fieldMapping.get("properties")).size(), equalTo(8));
                    assertThat(XContentMapValues.extractValue("properties.node.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.id.type", fieldMapping), equalTo("long"));
                    assertThat(XContentMapValues.extractValue("properties.type.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.action.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.description.type", fieldMapping), equalTo("keyword"));
                    assertThat(XContentMapValues.extractValue("properties.start_time_in_millis.type", fieldMapping), equalTo("date"));
                    assertThat(XContentMapValues.extractValue("properties.cancellable.type", fieldMapping), equalTo("boolean"));
                } else {
                    // Manual test specific object fields and if not just fail:
                    fail("unexpected field value type [" + fieldValue.getClass() + "] for field [" + fieldName + "]");
                }
            }
        }
    }
}
