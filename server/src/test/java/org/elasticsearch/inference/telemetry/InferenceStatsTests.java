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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.inference.telemetry.InferenceStats.ERROR_TYPE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.PRODUCTION_RELEASE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.SERVICE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.STACK_VERSION_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.STATUS_CODE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.TASK_TYPE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.create;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceStatsTests extends ESTestCase {

    private static final String TEST_STACK_VERSION = "8.99.0";
    private static final boolean TEST_IS_PRODUCTION_RELEASE = true;
    private static final String TEST_SERVICE = "service";
    private static final String TEST_MODEL_ID = "modelId";

    public static InferenceStats mockInferenceStats() {
        return new InferenceStats(mock(), mock(), mock(), Map.of());
    }

    public void testRecordWithService() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testLongCounter_WithNullThrowable_Returns200() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(null).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testLongHistogram_WithNullThrowable_Returns200() {
        var expectedDuration = randomLong();
        var longHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), longHistogram, mock(), Map.of());

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(null).record(expectedDuration);

        verify(longHistogram).record(
            eq(expectedDuration),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testCreation() {
        assertNotNull(create(MeterRegistry.NOOP, TEST_STACK_VERSION, TEST_IS_PRODUCTION_RELEASE));
    }

    public void testConstantAttributesIncludedInRequestCountMetrics() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(
            longCounter,
            mock(),
            mock(),
            Map.of(STACK_VERSION_ATTRIBUTE, TEST_STACK_VERSION, PRODUCTION_RELEASE_ATTRIBUTE, TEST_IS_PRODUCTION_RELEASE)
        );

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    STACK_VERSION_ATTRIBUTE,
                    TEST_STACK_VERSION,
                    PRODUCTION_RELEASE_ATTRIBUTE,
                    TEST_IS_PRODUCTION_RELEASE,
                    STATUS_CODE_ATTRIBUTE,
                    200
                )
            )
        );
    }

    public void testCounterWithElasticsearchStatusException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    STATUS_CODE_ATTRIBUTE,
                    statusCode.getStatus(),
                    ERROR_TYPE_ATTRIBUTE,
                    expectedError
                )
            )
        );
    }

    public void testCounterWithOtherException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), ERROR_TYPE_ATTRIBUTE, expectedError))
        );
    }

    public void testCounterWithUnknownModelAndElasticsearchStatusException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.requestCount().withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(STATUS_CODE_ATTRIBUTE, statusCode.getStatus(), ERROR_TYPE_ATTRIBUTE, expectedError))
        );
    }

    public void testCounterWithUnknownModelAndOtherException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.requestCount().withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(ERROR_TYPE_ATTRIBUTE, expectedError)));
    }

    public void testCounterWithCustomAttribute() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount()
            .withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID))
            .withAttribute("custom_key", "custom_value")
            .incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    STATUS_CODE_ATTRIBUTE,
                    200,
                    "custom_key",
                    "custom_value"
                )
            )
        );
    }

    public void testRecordDurationWithoutError() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
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

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    STATUS_CODE_ATTRIBUTE,
                    statusCode.getStatus(),
                    ERROR_TYPE_ATTRIBUTE,
                    expectedError
                )
            )
        );
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

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), ERROR_TYPE_ATTRIBUTE, expectedError))
        );
    }

    public void testRecordDurationWithUnknownModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.inferenceDuration().withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(Map.of(STATUS_CODE_ATTRIBUTE, statusCode.getStatus(), ERROR_TYPE_ATTRIBUTE, expectedError))
        );
    }

    public void testRecordDurationWithUnknownModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.inferenceDuration().withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(eq(expectedLong), eq(Map.of(ERROR_TYPE_ATTRIBUTE, expectedError)));
    }

    public void testConstantAttributesIncludedInDurationMetrics() {
        var expectedDuration = randomLong();
        var longHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(
            mock(),
            longHistogram,
            mock(),
            Map.of(STACK_VERSION_ATTRIBUTE, TEST_STACK_VERSION, PRODUCTION_RELEASE_ATTRIBUTE, TEST_IS_PRODUCTION_RELEASE)
        );

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).record(expectedDuration);

        verify(longHistogram).record(
            eq(expectedDuration),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    STACK_VERSION_ATTRIBUTE,
                    TEST_STACK_VERSION,
                    PRODUCTION_RELEASE_ATTRIBUTE,
                    TEST_IS_PRODUCTION_RELEASE,
                    STATUS_CODE_ATTRIBUTE,
                    200
                )
            )
        );
    }

    public void testDeploymentDuration() {
        var expectedDuration = randomLong();
        var deploymentHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), mock(), deploymentHistogram, Map.of());

        stats.deploymentDuration().withModel(model(TEST_SERVICE, TaskType.ANY, TEST_MODEL_ID)).record(expectedDuration);

        verify(deploymentHistogram).record(
            eq(expectedDuration),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
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
