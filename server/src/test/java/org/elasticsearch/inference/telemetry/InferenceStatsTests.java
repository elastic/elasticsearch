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
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.inference.telemetry.InferenceStats.INFERENCE_DEPLOYMENT_DURATION;
import static org.elasticsearch.inference.telemetry.InferenceStats.INFERENCE_REQUEST_COUNT_TOTAL;
import static org.elasticsearch.inference.telemetry.InferenceStats.INFERENCE_REQUEST_DURATION;
import static org.elasticsearch.inference.telemetry.InferenceStats.INFERENCE_SOURCE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.OTHER_VALUE;
import static org.elasticsearch.inference.telemetry.InferenceStats.SECURITY_AI_ASSISTANT_USE_CASE;
import static org.elasticsearch.inference.telemetry.InferenceStats.SERVICE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.SIEM_MIGRATIONS;
import static org.elasticsearch.inference.telemetry.InferenceStats.SIEM_MIGRATIONS_PREFIX;
import static org.elasticsearch.inference.telemetry.InferenceStats.STATUS_CODE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.TASK_TYPE_ATTRIBUTE;
import static org.elasticsearch.inference.telemetry.InferenceStats.create;
import static org.elasticsearch.telemetry.metric.MetricAttributes.ERROR_TYPE;
import static org.elasticsearch.telemetry.metric.MetricAttributes.ES_PRODUCTION_RELEASE;
import static org.elasticsearch.telemetry.metric.MetricAttributes.ES_PRODUCT_ORIGIN;
import static org.elasticsearch.telemetry.metric.MetricAttributes.ES_STACK_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceStatsTests extends ESTestCase {

    private static final String TEST_STACK_VERSION = "8.99.0";
    private static final boolean TEST_IS_PRODUCTION_RELEASE = true;
    private static final String TEST_SERVICE = "service";
    private static final String TEST_PRODUCT_ORIGIN = "kibana";

    public static InferenceStats mockInferenceStats() {
        return new InferenceStats(mock(), mock(), mock(), Map.of());
    }

    public void testRecordWithService() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withSuccess().incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testCounterWithNoStatusSet() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).incrementBy(1);

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString())));
    }

    public void testDurationWithNoStatusSet() {
        var expectedDuration = randomLong();
        var longHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), longHistogram, mock(), Map.of());

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).record(expectedDuration);

        verify(longHistogram).record(
            eq(expectedDuration),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString()))
        );
    }

    public void testLongCounter_WithNullThrowable_Returns200() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(null).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testLongHistogram_WithNullThrowable_Returns200() {
        var expectedDuration = randomLong();
        var longHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), longHistogram, mock(), Map.of());

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(null).record(expectedDuration);

        verify(longHistogram).record(
            eq(expectedDuration),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testCreation() {
        var mockRegistry = mock(MeterRegistry.class);
        when(mockRegistry.registerLongCounter(any(), any(), any())).thenReturn(mock(LongCounter.class));
        when(mockRegistry.registerLongHistogram(any(), any(), any())).thenReturn(mock(LongHistogram.class));

        create(mockRegistry, TEST_STACK_VERSION, TEST_IS_PRODUCTION_RELEASE);
        verify(mockRegistry, times(1)).registerLongCounter(eq(INFERENCE_REQUEST_COUNT_TOTAL), any(), any());
        verify(mockRegistry, times(1)).registerLongHistogram(eq(INFERENCE_REQUEST_DURATION), any(), any());
        verify(mockRegistry, times(1)).registerLongHistogram(eq(INFERENCE_DEPLOYMENT_DURATION), any(), any());
    }

    public void testConstantAttributesIncludedInRequestCountMetrics() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(
            longCounter,
            mock(),
            mock(),
            Map.of(ES_STACK_VERSION, TEST_STACK_VERSION, ES_PRODUCTION_RELEASE, TEST_IS_PRODUCTION_RELEASE)
        );

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withSuccess().incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    ES_STACK_VERSION,
                    TEST_STACK_VERSION,
                    ES_PRODUCTION_RELEASE,
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

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(exception).incrementBy(1);

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
                    ERROR_TYPE,
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

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), ERROR_TYPE, expectedError))
        );
    }

    public void testCounterWithoutModelAndElasticsearchStatusException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.requestCount().withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(STATUS_CODE_ATTRIBUTE, statusCode.getStatus(), ERROR_TYPE, expectedError)));
    }

    public void testCounterWithoutModelAndOtherException() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.requestCount().withThrowable(exception).incrementBy(1);

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(ERROR_TYPE, expectedError)));
    }

    public void testCounterWithCustomAttribute() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        var customKey = "custom_key";
        var customValue = "custom_value";

        stats.requestCount()
            .withModel(model(TEST_SERVICE, TaskType.ANY))
            .withSuccess()
            .withAttribute(customKey, customValue)
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
                    customKey,
                    customValue
                )
            )
        );
    }

    public void testRecordDurationWithoutError() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withSuccess().record(expectedLong);

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

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(exception).record(expectedLong);

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
                    ERROR_TYPE,
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

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), ERROR_TYPE, expectedError))
        );
    }

    public void testRecordDurationWithoutModelAndElasticsearchStatusException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var statusCode = RestStatus.BAD_REQUEST;
        var exception = new ElasticsearchStatusException("hello", statusCode);
        var expectedError = String.valueOf(statusCode.getStatus());

        stats.inferenceDuration().withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(
            eq(expectedLong),
            eq(Map.of(STATUS_CODE_ATTRIBUTE, statusCode.getStatus(), ERROR_TYPE, expectedError))
        );
    }

    public void testRecordDurationWithoutModelAndOtherException() {
        var expectedLong = randomLong();
        var histogramCounter = mock(LongHistogram.class);
        var stats = new InferenceStats(mock(), histogramCounter, mock(), Map.of());
        var exception = new IllegalStateException("ahh");
        var expectedError = exception.getClass().getSimpleName();

        stats.inferenceDuration().withThrowable(exception).record(expectedLong);

        verify(histogramCounter).record(eq(expectedLong), eq(Map.of(ERROR_TYPE, expectedError)));
    }

    public void testConstantAttributesIncludedInDurationMetrics() {
        var expectedDuration = randomLong();
        var longHistogram = mock(LongHistogram.class);
        var stats = new InferenceStats(
            mock(),
            longHistogram,
            mock(),
            Map.of(ES_STACK_VERSION, TEST_STACK_VERSION, ES_PRODUCTION_RELEASE, TEST_IS_PRODUCTION_RELEASE)
        );

        stats.inferenceDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withSuccess().record(expectedDuration);

        verify(longHistogram).record(
            eq(expectedDuration),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    ES_STACK_VERSION,
                    TEST_STACK_VERSION,
                    ES_PRODUCTION_RELEASE,
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

        stats.deploymentDuration().withModel(model(TEST_SERVICE, TaskType.ANY)).withSuccess().record(expectedDuration);

        verify(deploymentHistogram).record(
            eq(expectedDuration),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), STATUS_CODE_ATTRIBUTE, 200))
        );
    }

    public void testWithProductContext_UseCase_Origin_Present() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext(SECURITY_AI_ASSISTANT_USE_CASE, TEST_PRODUCT_ORIGIN);

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    SECURITY_AI_ASSISTANT_USE_CASE,
                    ES_PRODUCT_ORIGIN,
                    TEST_PRODUCT_ORIGIN
                )
            )
        );
    }

    public void testWithProductContext_OnlyProductUseCase() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext(SECURITY_AI_ASSISTANT_USE_CASE, null);

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    SECURITY_AI_ASSISTANT_USE_CASE
                )
            )
        );
    }

    public void testWithProductContext_OnlyProductOrigin() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext(null, TEST_PRODUCT_ORIGIN);

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    ES_PRODUCT_ORIGIN,
                    TEST_PRODUCT_ORIGIN
                )
            )
        );
    }

    public void testWithProductContext_Empty() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(InferenceProductContext.EMPTY).incrementBy(1);

        verify(longCounter).incrementBy(eq(1L), eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString())));
    }

    public void testWithProductUseCase() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var useCase = "semantic_text_bulk";

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductUseCase(useCase).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(Map.of(SERVICE_ATTRIBUTE, TEST_SERVICE, TASK_TYPE_ATTRIBUTE, TaskType.ANY.toString(), INFERENCE_SOURCE_ATTRIBUTE, useCase))
        );
    }

    public void testWithProductContext_UnknownUseCase_BucketsAsOther() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext("some-bogus-use-case", TEST_PRODUCT_ORIGIN);

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    OTHER_VALUE,
                    ES_PRODUCT_ORIGIN,
                    TEST_PRODUCT_ORIGIN
                )
            )
        );
    }

    public void testWithProductContext_UnknownOrigin_BucketsAsOther() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext(SECURITY_AI_ASSISTANT_USE_CASE, "some-bogus-origin");

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    SECURITY_AI_ASSISTANT_USE_CASE,
                    ES_PRODUCT_ORIGIN,
                    OTHER_VALUE
                )
            )
        );
    }

    public void testWithProductUseCase_UnknownValue_BucketsAsOther() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductUseCase("some-bogus-use-case").incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    OTHER_VALUE
                )
            )
        );
    }

    public void testWithProductContext_MixedCaseInput_NormalizedToLowercase() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext("Security_AI_Assistant", "KIBANA");

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    SECURITY_AI_ASSISTANT_USE_CASE,
                    ES_PRODUCT_ORIGIN,
                    TEST_PRODUCT_ORIGIN
                )
            )
        );
    }

    public void testWithProductContext_SiemMigrationsVariant_CollapsedToBase() {
        var longCounter = mock(LongCounter.class);
        var stats = new InferenceStats(longCounter, mock(), mock(), Map.of());
        var ctx = new InferenceProductContext(SIEM_MIGRATIONS_PREFIX + randomAlphaOfLength(8), TEST_PRODUCT_ORIGIN);

        stats.requestCount().withModel(model(TEST_SERVICE, TaskType.ANY)).withProductContext(ctx).incrementBy(1);

        verify(longCounter).incrementBy(
            eq(1L),
            eq(
                Map.of(
                    SERVICE_ATTRIBUTE,
                    TEST_SERVICE,
                    TASK_TYPE_ATTRIBUTE,
                    TaskType.ANY.toString(),
                    INFERENCE_SOURCE_ATTRIBUTE,
                    SIEM_MIGRATIONS,
                    ES_PRODUCT_ORIGIN,
                    TEST_PRODUCT_ORIGIN
                )
            )
        );
    }

    public void testKnownAllowlistEntriesAreLowercase() {
        for (var value : InferenceStats.KNOWN_PRODUCT_USE_CASES) {
            assertEquals("KNOWN_INFERENCE_SOURCES entry must be lowercase: " + value, value.toLowerCase(java.util.Locale.ROOT), value);
        }
        for (var value : MetricAttributes.KNOWN_PRODUCT_ORIGINS) {
            assertEquals("KNOWN_PRODUCT_ORIGINS entry must be lowercase: " + value, value.toLowerCase(java.util.Locale.ROOT), value);
        }
    }

    private Model model(String service, TaskType taskType) {
        var configuration = mock(ModelConfigurations.class);
        when(configuration.getService()).thenReturn(service);
        var settings = mock(ServiceSettings.class);

        var model = mock(Model.class);
        when(model.getTaskType()).thenReturn(taskType);
        when(model.getConfigurations()).thenReturn(configuration);
        when(model.getServiceSettings()).thenReturn(settings);

        return model;
    }
}
