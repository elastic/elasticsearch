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
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class KibanaPluginTests extends ESTestCase {

    public void testKibanaIndexNames() {
        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY).stream().map(SystemIndexDescriptor::getIndexPattern).toList(),
            contains(
                ".kibana_*",
                ".reporting-*",
                ".chat-*",
                KibanaPlugin.WORKFLOWS_SYSTEM_INDEX_PATTERN,
                ".apm-agent-configuration*",
                ".apm-custom-link*"
            )
        );
    }

    public void testWorkflowsDataStreamsRegistered() {
        assertThat(
            new KibanaPlugin().getSystemDataStreamDescriptors().stream().map(SystemDataStreamDescriptor::getDataStreamName).toList(),
            contains(KibanaPlugin.WORKFLOWS_EVENTS_DATA_STREAM_NAME, KibanaPlugin.WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME)
        );
    }

    public void testNoSystemIndexPatternCoversWorkflowsDataStreams() {
        var indexDescriptors = new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY);
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-events")));
        assertFalse(indexDescriptors.stream().anyMatch(d -> d.matchesIndexPattern(".workflows-execution-data-stream-logs")));
    }

    public void testWorkflowsSystemIndexDescriptorCoversOtherWorkflowsIndices() {
        assertTrue(KibanaPlugin.WORKFLOWS_INDEX_DESCRIPTOR.matchesIndexPattern(".workflows-internal"));
        assertTrue(KibanaPlugin.WORKFLOWS_INDEX_DESCRIPTOR.matchesIndexPattern(".workflows-state"));
    }

    public void testKibanaFeaturePassesSystemIndicesOverlapChecks() {
        SystemIndices.Feature feature = SystemIndices.Feature.fromSystemIndexPlugin(new KibanaPlugin(), Settings.EMPTY);
        assertNotNull(new SystemIndices(List.of(feature)));
    }
}
