/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;

public class KibanaPluginTests extends ESTestCase {

    public void testKibanaIndexNames() {
        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY).stream().map(SystemIndexDescriptor::getIndexPattern).toList(),
            contains(
                KibanaPlugin.KIBANA_SYSTEM_INDEX_PATTERN,
                ".reporting-*",
                ".chat-*",
                ".context-engine-*",
                ".contextengine-*",
                KibanaPlugin.WORKFLOWS_SYSTEM_INDEX_PATTERN,
                ".apm-agent-configuration*",
                ".apm-custom-link*"
            )
        );
    }

    public void testKibanaDataStreamsRegistered() {
        assertThat(
            new KibanaPlugin().getSystemDataStreamDescriptors().stream().map(SystemDataStreamDescriptor::getDataStreamName).toList(),
            contains(
                KibanaPlugin.WORKFLOWS_EVENTS_DATA_STREAM_NAME,
                KibanaPlugin.WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME,
                KibanaPlugin.CHANGE_HISTORY_DATA_STREAM_NAME,
                KibanaPlugin.NOTIFICATION_CENTER_DATA_STREAM_NAME
            )
        );
    }

    public void testNoSystemIndexPatternCoversRegisteredDataStreams() {
        var indexDescriptors = new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY);
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-events")));
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-execution-data-stream-logs")));
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".kibana_change_history")));
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".kibana_change_history-000001")));
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".kibana-notifications")));
    }

    public void testWorkflowsSystemIndexDescriptorCoversOtherWorkflowsIndices() {
        assertTrue(KibanaPlugin.WORKFLOWS_INDEX_DESCRIPTOR.matchesIndexPattern(".workflows-internal"));
        assertTrue(KibanaPlugin.WORKFLOWS_INDEX_DESCRIPTOR.matchesIndexPattern(".workflows-state"));
        assertTrue(KibanaPlugin.WORKFLOWS_INDEX_DESCRIPTOR.matchesIndexPattern(".workflows-workflows"));
    }

    public void testWorkflowsExecutionIndicesAreNotSystemIndices() {
        var indexDescriptors = new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY);
        assertFalse(
            "'.workflows-executions' must not be a system index after the carve-out",
            indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-executions"))
        );
        assertFalse(
            "'.workflows-step-executions' must not be a system index after the carve-out",
            indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-step-executions"))
        );
    }

    public void testKibanaSystemIndexDescriptorStillCoversKibanaSavedObjects() {
        assertTrue(KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(".kibana_1"));
        assertTrue(KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(".kibana_alerting_cases"));
        assertTrue(KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(".kibana_task_manager_8.0.0_001"));
        assertFalse(KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(".kibana_change_history"));
        assertFalse(KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(".kibana_change_history-2026.07.16-000001"));
    }

    /**
     * The notification center read path filters on these fields; they exist for Kibana's access-control
     * filter and nothing in Elasticsearch reads them, so only this test would catch their removal.
     * {@code space_ids} is plural because a notification can belong to several spaces at once.
     * {@code profile_uid} is singular: a targeted notification is fanned out to one document per
     * recipient, so that each recipient's copy carries its own {@code notification_id}.
     */
    public void testNotificationCenterMapsAccessControlFields() {
        Map<String, Object> properties = notificationCenterMappedProperties();
        assertThat(properties, hasEntry("space_ids", Map.of("type", "keyword")));
        assertThat(properties, hasEntry("profile_uid", Map.of("type", "keyword")));
    }

    /**
     * An {@code ignore_above} value leaves an over-long value unindexed, and an {@code exists} query
     * then reports the field as absent. Absent {@code profile_uid} is how the read path spells
     * "broadcast", so capping the field would make an over-long recipient id visible to everyone.
     * Without the cap the document is rejected at index time instead.
     */
    public void testNotificationCenterAccessControlFieldsAreNotCapped() {
        Map<String, Object> properties = notificationCenterMappedProperties();
        for (String field : List.of("space_ids", "profile_uid")) {
            assertFalse(field + " must not set ignore_above", asMap(properties.get(field)).containsKey("ignore_above"));
        }
    }

    /**
     * Per-type privilege requirements are declared in Kibana's notification registry and evaluated
     * there, so no privilege string is ever written to or read from a document.
     */
    public void testNotificationCenterDoesNotMapAPrivilegeField() {
        Map<String, Object> properties = notificationCenterMappedProperties();
        assertFalse(properties.containsKey("required_privilege"));
        assertFalse(properties.containsKey("visible_with_privilege"));
    }

    private static Map<String, Object> notificationCenterMappedProperties() {
        SystemDataStreamDescriptor descriptor = new KibanaPlugin().getSystemDataStreamDescriptors()
            .stream()
            .filter(d -> KibanaPlugin.NOTIFICATION_CENTER_DATA_STREAM_NAME.equals(d.getDataStreamName()))
            .findFirst()
            .orElseThrow();
        Map<String, Object> mappings = XContentHelper.convertToMap(
            descriptor.getComposableIndexTemplate().template().mappings().uncompressed(),
            true,
            XContentType.JSON
        ).v2();
        return asMap(mappings.get("properties"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    public void testKibanaFeaturePassesSystemIndicesOverlapChecks() {
        SystemIndices.Feature feature = SystemIndices.Feature.fromSystemIndexPlugin(new KibanaPlugin(), Settings.EMPTY);
        assertNotNull(new SystemIndices(List.of(feature)));
    }
}
