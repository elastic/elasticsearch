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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * OpenTelemetry-backed inference metrics.
 */
public class InferenceStats {

    // These attributes predated the "es_" prefix requirement
    public static final String SERVICE_ATTRIBUTE = "service";
    public static final String TASK_TYPE_ATTRIBUTE = "task_type";
    public static final String STATUS_CODE_ATTRIBUTE = "status_code";
    public static final String INFERENCE_SOURCE_ATTRIBUTE = "inference_source";
    public static final String INFERENCE_REQUEST_COUNT_TOTAL = "es.inference.requests.count.total";
    public static final String INFERENCE_REQUEST_DURATION = "es.inference.requests.time";
    public static final String INFERENCE_DEPLOYMENT_DURATION = "es.inference.trained_model.deployment.time";
    // Attribute keys must start with "es_" prefix see org.elasticsearch.telemetry.apm.internal.MetricValidator#validateAttributeKey
    static final String STACK_VERSION_ATTRIBUTE = "es_stack_version";
    // Indicates whether the node is a production release (i.e. not a snapshot, alpha, etc.)
    static final String PRODUCTION_RELEASE_ATTRIBUTE = "es_production_release";
    private static final Logger logger = LogManager.getLogger(InferenceStats.class);
    private final LongCounter requestCountInstrument;
    private final LongHistogram inferenceDurationInstrument;
    private final LongHistogram deploymentDurationInstrument;
    private final Map<String, Object> constantAttributes;

    /**
     * This should be an internal constructor. It's public to enable testing.
     *
     * @param requestCountInstrument       long counter for inference request counts
     * @param inferenceDurationInstrument  long histogram for inference request latency
     * @param deploymentDurationInstrument long histogram for trained model deployment wait time
     * @param constantAttributes           attributes to be included with every metric recorded by this instance,
     *                                     such as the node's stack version and whether it's a production release
     *                                     (i.e. not a snapshot, alpha, etc.)
     */
    public InferenceStats(
        LongCounter requestCountInstrument,
        LongHistogram inferenceDurationInstrument,
        LongHistogram deploymentDurationInstrument,
        Map<String, Object> constantAttributes
    ) {
        this.requestCountInstrument = Objects.requireNonNull(requestCountInstrument);
        this.inferenceDurationInstrument = Objects.requireNonNull(inferenceDurationInstrument);
        this.deploymentDurationInstrument = Objects.requireNonNull(deploymentDurationInstrument);
        this.constantAttributes = Objects.requireNonNull(constantAttributes);
    }

    public static InferenceStats create(MeterRegistry meterRegistry, String stackVersion, boolean isProductionRelease) {
        return new InferenceStats(
            meterRegistry.registerLongCounter(
                INFERENCE_REQUEST_COUNT_TOTAL,
                "Inference API request counts for a particular service and task type",
                "operations"
            ),
            meterRegistry.registerLongHistogram(
                INFERENCE_REQUEST_DURATION,
                "Inference API request duration for a particular service and task type",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                INFERENCE_DEPLOYMENT_DURATION,
                "Inference API time spent waiting for Trained Model Deployments",
                "ms"
            ),
            Map.of(STACK_VERSION_ATTRIBUTE, stackVersion, PRODUCTION_RELEASE_ATTRIBUTE, isProductionRelease)
        );
    }

    /**
     * Returns a request counter instrument builder. Call {@link CounterBuilder#withSuccess()}
     * or {@link CounterBuilder#withThrowable(Throwable)} to set the outcome before calling the terminal
     * {@link CounterBuilder#incrementBy}. Omitting both records no {@code status_code},
     * which is appropriate when counting a request attempt before the outcome is known.
     */
    public CounterBuilder requestCount() {
        return new CounterBuilder(requestCountInstrument, constantAttributes);
    }

    /**
     * Returns a request duration histogram instrument builder. Call {@link CounterBuilder#withSuccess()} or
     * {@link DurationBuilder#withThrowable(Throwable)} to set the outcome before calling the terminal
     * {@link DurationBuilder#record(long)}. Omitting both records no {@code status_code}.
     */
    public DurationBuilder inferenceDuration() {
        return new DurationBuilder(inferenceDurationInstrument, constantAttributes);
    }

    /**
     * Returns a deployment duration histogram instrument builder. Call {@link CounterBuilder#withSuccess()} or
     * {@link DurationBuilder#withThrowable(Throwable)} to set the outcome before calling the terminal
     * {@link DurationBuilder#record(long)}. Omitting both records no {@code status_code}.
     */
    public DurationBuilder deploymentDuration() {
        return new DurationBuilder(deploymentDurationInstrument, constantAttributes);
    }

    abstract static class AbstractBuilder<B extends AbstractBuilder<B>> {

        protected final Map<String, Object> attributes;

        AbstractBuilder(Map<String, Object> constantAttributes) {
            this.attributes = new HashMap<>(constantAttributes);
        }

        public B withModel(Model model) {
            attributes.put(SERVICE_ATTRIBUTE, model.getConfigurations().getService());
            attributes.put(TASK_TYPE_ATTRIBUTE, model.getTaskType().toString());

            return cast();
        }

        public B withSuccess() {
            attributes.put(STATUS_CODE_ATTRIBUTE, 200);

            return cast();
        }

        /**
         * Records failure attributes from the given throwable. If {@code null} is passed this behaves identically to
         * {@link #withSuccess()} and sets {@code status_code=200}. Prefer {@link #withSuccess()} when the outcome is known to be
         * successful.
         */
        public B withThrowable(@Nullable Throwable throwable) {
            if (throwable == null) {
                return withSuccess();
            }
            applyThrowable(throwable, attributes);

            return cast();
        }

        public B withAttribute(String key, Object value) {
            attributes.put(key, value);

            return cast();
        }

        @SuppressWarnings("unchecked")
        private B cast() {
            return (B) this;
        }
    }

    /**
     * Builder for {@link LongCounter} metrics. Call {@link #withSuccess()} or {@link #withThrowable(Throwable)} to set the outcome.
     * Omitting both leaves the metric without a {@code status_code} attribute, which is correct when recording a request attempt
     * before the outcome is known.
     *
     * <p>Example usage:
     * <pre>{@code
     * inferenceStats.requestCount()
     *     .withModel(model)
     *     .withSuccess()
     *     .incrementBy(1);
     * }</pre>
     */
    public static class CounterBuilder extends AbstractBuilder<CounterBuilder> {

        private final LongCounter counter;

        CounterBuilder(LongCounter counter, Map<String, Object> constantAttributes) {
            super(constantAttributes);
            this.counter = counter;
        }

        public void incrementBy(long value) {
            try {
                counter.incrementBy(value, attributes);
            } catch (Exception e) {
                logger.debug("Failed to record inference counter metric for [{}]", counter.getName(), e);
            }
        }
    }

    /**
     * Builder for {@link LongHistogram} metrics. Call {@link #withSuccess()} or {@link #withThrowable(Throwable)} to set the outcome.
     * Omitting both leaves the metric without a {@code status_code} attribute, which is correct when recording duration before the
     * outcome is known.
     *
     * <p>Example usage:
     * <pre>{@code
     * inferenceStats.inferenceDuration()
     *     .withModel(model)
     *     .withThrowable(throwable)
     *     .record(durationMs);
     *
     * inferenceStats.deploymentDuration()
     *     .withModel(model)
     *     .withSuccess()
     *     .record(durationMs);
     * }</pre>
     */
    public static class DurationBuilder extends AbstractBuilder<DurationBuilder> {

        private final LongHistogram histogram;

        DurationBuilder(LongHistogram histogram, Map<String, Object> constantAttributes) {
            super(constantAttributes);
            this.histogram = histogram;
        }

        public void record(long durationMs) {
            try {
                histogram.record(durationMs, attributes);
            } catch (Exception e) {
                logger.debug("Failed to record inference duration metric for [{}]", histogram.getName(), e);
            }
        }
    }

    private static void applyThrowable(Throwable throwable, Map<String, Object> attributes) {
        if (throwable instanceof ElasticsearchStatusException ese) {
            attributes.put(STATUS_CODE_ATTRIBUTE, ese.status().getStatus());
            attributes.put(MetricAttributes.ERROR_TYPE, String.valueOf(ese.status().getStatus()));
        } else {
            attributes.put(MetricAttributes.ERROR_TYPE, throwable.getClass().getSimpleName());
        }
    }
}
