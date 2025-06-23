/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.create;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.modelAttributes;
import static org.elasticsearch.xpack.inference.telemetry.InferenceStats.responseAttributes;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceStatsTests extends ESTestCase {

    public void testRecordWithModel() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock());

        stats.requestCount().incrementBy(1, modelAttributes(model("service", TaskType.ANY, "modelId")));

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of("service", "service", "task_type", TaskType.ANY.toString(), "model_id", "modelId"))
        );
    }

    public void testRecordWithoutModel() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock());

        stats.requestCount().incrementBy(1, modelAttributes(model("service", TaskType.ANY, null)));

        verify(longCounter).incrementBy(eq(1L), eq(Map.of("service", "service", "task_type", TaskType.ANY.toString())));
    }

    public void testCreation() {
        assertNotNull(create(MeterRegistry.NOOP));
    }

    public void testRecordDurationWithoutError() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(modelAttributes(model("service", TaskType.ANY, "modelId")));
        metricAttributes.putAll(responseAttributes(null));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), is("service"));
            assertThat(attributes.get("task_type"), is(TaskType.ANY.toString()));
            assertThat(attributes.get("model_id"), is("modelId"));
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error.type"), nullValue());
        }));
    }

    /**
     * "If response status code was sent or received and status indicates an error according to HTTP span status definition,
     * error.type SHOULD be set to the status code number (represented as a string)"
     * - https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
     */
    public void testRecordDurationWithElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(modelAttributes(model("service", TaskType.ANY, "modelId")));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), is("service"));
            assertThat(attributes.get("task_type"), is(TaskType.ANY.toString()));
            assertThat(attributes.get("model_id"), is("modelId"));
            assertThat(attributes.get("status_code"), is(statusCode.getStatus()));
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    /**
     * "If the request fails with an error before response status code was sent or received,
     * error.type SHOULD be set to exception type"
     * - https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
     */
    public void testRecordDurationWithOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(modelAttributes(model("service", TaskType.ANY, "modelId")));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), is("service"));
            assertThat(attributes.get("task_type"), is(TaskType.ANY.toString()));
            assertThat(attributes.get("model_id"), is("modelId"));
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnparsedModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        var unparsedModel = new UnparsedModel("inferenceEntityId", TaskType.ANY, "service", Map.of(), Map.of());

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(modelAttributes(unparsedModel));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), is("service"));
            assertThat(attributes.get("task_type"), is(TaskType.ANY.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(statusCode.getStatus()));
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnparsedModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        var unparsedModel = new UnparsedModel("inferenceEntityId", TaskType.ANY, "service", Map.of(), Map.of());

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(modelAttributes(unparsedModel));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), is("service"));
            assertThat(attributes.get("task_type"), is(TaskType.ANY.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnknownModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.inferenceDuration().record(expectedLong, responseAttributes(exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), nullValue());
            assertThat(attributes.get("task_type"), nullValue());
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(statusCode.getStatus()));
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnknownModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter);
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.inferenceDuration().record(expectedLong, responseAttributes(exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get("service"), nullValue());
            assertThat(attributes.get("task_type"), nullValue());
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
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
