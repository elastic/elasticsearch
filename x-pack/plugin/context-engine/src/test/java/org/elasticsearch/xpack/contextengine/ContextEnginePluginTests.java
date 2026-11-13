/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ContextEnginePluginTests extends ESTestCase {

    public void testSystemDataStreamsRegistered() {
        assertThat(
            new ContextEnginePlugin(Settings.EMPTY).getSystemDataStreamDescriptors()
                .stream()
                .map(SystemDataStreamDescriptor::getDataStreamName)
                .toList(),
            contains(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME, ContextEnginePlugin.SESSION_EVENTS_DATA_STREAM_NAME)
        );
    }

    public void testSystemDataStreamDescriptors() {
        for (SystemDataStreamDescriptor descriptor : new ContextEnginePlugin(Settings.EMPTY).getSystemDataStreamDescriptors()) {
            assertThat(descriptor.isExternal(), is(false));
            assertThat(descriptor.getOrigin(), equalTo(CONTEXT_ENGINE_ORIGIN));

            ComposableIndexTemplate template = descriptor.getComposableIndexTemplate();
            assertThat(template.indexPatterns(), contains(descriptor.getDataStreamName()));
            assertThat(template.getDataStreamTemplate().isHidden(), is(true));
            assertThat(template.template().lifecycle().dataRetention().get(), equalTo(TimeValue.timeValueDays(90)));
            assertThat(template.template().settings().get("index.sort.field"), equalTo("@timestamp"));
            assertThat(template.template().settings().get("index.sort.order"), equalTo("desc"));
            assertThat(template.template().mappings().string(), containsString("\"dynamic\":\"strict\""));
        }
    }

    public void testSessionMemoryInferenceId() {
        String inferenceId = randomAlphaOfLength(10);
        Settings settings = Settings.builder().put(ContextEnginePlugin.SESSION_MEMORY_INFERENCE_ID.getKey(), inferenceId).build();
        String mappings = new ContextEnginePlugin(settings).getSystemDataStreamDescriptors()
            .stream()
            .filter(d -> d.getDataStreamName().equals(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME))
            .findFirst()
            .orElseThrow()
            .getComposableIndexTemplate()
            .template()
            .mappings()
            .string();
        assertThat(mappings, containsString("\"inference_id\":\"" + inferenceId + "\""));
        assertThat(mappings, not(containsString("${")));
    }

    public void testFeatureRegistersWithoutOverlap() {
        ContextEnginePlugin plugin = new ContextEnginePlugin(Settings.EMPTY);
        SystemIndices systemIndices = new SystemIndices(List.of(SystemIndices.Feature.fromSystemIndexPlugin(plugin, Settings.EMPTY)));
        assertThat(systemIndices.getFeatureNames().contains(ContextEnginePlugin.FEATURE_NAME), is(true));
        assertThat(systemIndices.isSystemDataStream(ContextEnginePlugin.SESSION_MEMORY_DATA_STREAM_NAME), is(true));
        assertThat(systemIndices.isSystemDataStream(ContextEnginePlugin.SESSION_EVENTS_DATA_STREAM_NAME), is(true));
    }
}
