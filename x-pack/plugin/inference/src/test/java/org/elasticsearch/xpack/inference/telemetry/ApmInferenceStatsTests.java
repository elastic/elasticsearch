/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApmInferenceStatsTests extends ESTestCase {

    public void testRecordWithModel() {
        var longCounter = mock(LongCounter.class);

        var stats = new ApmInferenceStats(longCounter);

        stats.incrementRequestCount(model("service", TaskType.ANY, "modelId"));

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of("service", "service", "task_type", TaskType.ANY.toString(), "model_id", "modelId"))
        );
    }

    public void testRecordWithoutModel() {
        var longCounter = mock(LongCounter.class);

        var stats = new ApmInferenceStats(longCounter);

        stats.incrementRequestCount(model("service", TaskType.ANY, null));

        verify(longCounter).incrementBy(eq(1L), eq(Map.of("service", "service", "task_type", TaskType.ANY.toString())));
    }

    public void testCreation() {
        assertNotNull(ApmInferenceStats.create(MeterRegistry.NOOP));
    }

    private Model model(String service, TaskType taskType, String modelId) {
        var configuration = mock(ModelConfigurations.class);
        when(configuration.getService()).thenReturn(service);
        var settings = mock(ServiceSettings.class);
        if (modelId != null) {
            when(settings.modelId()).thenReturn(modelId);
        }

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(taskType);
        when(model.getConfigurations()).thenReturn(configuration);
        when(model.getServiceSettings()).thenReturn(settings);

        return model;
    }
}
