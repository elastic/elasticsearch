/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

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

import static org.elasticsearch.inference.telemetry.InferenceStats.ERROR_TYPE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.PRODUCTION_RELEASE;
import static org.elasticsearch.inference.telemetry.InferenceStats.SERVICE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.STACK_VERSION_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.STATUS_CODE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.TASK_TYPE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.create;
import static org.elasticsearch.inference.telemetry.InferenceStats.responseAttributes;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceStatsTests extends ESTestCase {

    private static final String TEST_STACK_VERSION = "8.99.0";
    private static final boolean TEST_IS_PRODUCTION_RELEASE = true;
    private static final String TEST_SERVICE = "service";
    private static final String TEST_MODEL_ID = "modelId";
    private static final String MODEL_ID_ATTRIBUTE = "model_id";
    private static final String TEST_INFERENCE_ENTITY_ID = "inferenceEntityId";

    public static InferenceStats mockInferenceStats() {
        return new InferenceStats(mock(), mock(), mock(), Map.of());
    }

    public void testRecordWithService() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().incrementBy(1, stats.serviceAttributes(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)));

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString())));
    }

    public void testRecordWithoutModel() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().incrementBy(1, stats.serviceAttributes(model(TEST_SERVICE, TaskType.ANY, null)));

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString())));
    }

    public void testCreation() {
        assertNotNull(create(MeterRegistry.NOOP, TEST_STACK_VERSION, TEST_IS_PRODUCTION_RELEASE));
    }

    public void testServiceAttributesIncludesConstantAttributes() {
        var stats = create(MeterRegistry.NOOP, TEST_STACK_VERSION, TEST_IS_PRODUCTION_RELEASE);

        var attributes = stats.serviceAttributes(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID));

        assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
        assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
        assertThat(attributes.get(STACK_VERSION_ATTRIBUTE), is(TEST_STACK_VERSION));
        assertThat(attributes.get(PRODUCTION_RELEASE), is(TEST_IS_PRODUCTION_RELEASE));
    }

    public void testRecordDurationWithoutError() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());

        stats.inferenceDuration()
            .record(expectedLong, stats.serviceAndResponseAttributes(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID), null));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), is(200));
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), nullValue());
        }));
    }

    /**
     * "If response status code was sent or received and status indicates an error according to HTTP span status definition,
     * error_type SHOULD be set to the status code number (represented as a string)"
     * - https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
     */
    public void testRecordDurationWithElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.inferenceDuration()
            .record(expectedLong, stats.serviceAndResponseAttributes(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID), exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), is(statusCode.getStatus()));
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
        }));
    }

    /**
     * "If the request fails with an error before response status code was sent or received,
     * error_type SHOULD be set to exception type"
     * - https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
     */
    public void testRecordDurationWithOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.inferenceDuration()
            .record(expectedLong, stats.serviceAndResponseAttributes(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID), exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnparsedModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        var unparsedModel = new UnparsedModel(TEST_INFERENCE_ENTITY_ID, TaskType.ANY, TEST_SERVICE, Map.of(), Map.of());

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(serviceAttributesFromUnparsedModel(unparsedModel));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
            assertThat(attributes.get(MODEL_ID_ATTRIBUTE), nullValue());
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), is(statusCode.getStatus()));
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnparsedModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        var unparsedModel = new UnparsedModel(TEST_INFERENCE_ENTITY_ID, TaskType.ANY, TEST_SERVICE, Map.of(), Map.of());

        Map<String, Object> metricAttributes = new HashMap<>();
        metricAttributes.putAll(serviceAttributesFromUnparsedModel(unparsedModel));
        metricAttributes.putAll(responseAttributes(exception));

        stats.inferenceDuration().record(expectedLong, metricAttributes);

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), is(TEST_SERVICE));
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), is(TaskType.ANY.toString()));
            assertThat(attributes.get(MODEL_ID_ATTRIBUTE), nullValue());
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnknownModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.inferenceDuration().record(expectedLong, responseAttributes(exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(MODEL_ID_ATTRIBUTE), nullValue());
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), is(statusCode.getStatus()));
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
        }));
    }

    public void testRecordDurationWithUnknownModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.inferenceDuration().record(expectedLong, responseAttributes(exception));

        verify(histogramCounter).record(eq(expectedLong), assertArg(attributes -> {
            assertThat(attributes.get(SERVICE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(TASK_TYPE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(MODEL_ID_ATTRIBUTE), nullValue());
            assertThat(attributes.get(STATUS_CODE_ATTRIBUTE), nullValue());
            assertThat(attributes.get(ERROR_TYPE_ATTRIBUTE), is(expectedError));
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

    private static Map<String, Object> serviceAttributesFromUnparsedModel(UnparsedModel model) {
        return Map.of(SERVICE_ATTRIBUTE, model.service(), TASK_TYPE_ATTRIBUTE, model.taskType().toString());
    }
}
