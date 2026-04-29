/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * OpenTelemetry-backed inference metrics.
 */
public class InferenceStats {

    static final String STACK_VERSION_ATTRIBUTE = "es_stack_version";
    static final String PRODUCTION_RELEASE_ATTRIBUTE = "es_production_release";
    static final String SERVICE_ATTRIBUTE = "service";
    static final String TASK_TYPE_ATTRIBUTE = "task_type";
    public static final String STATUS_CODE_ATTRIBUTE = "status_code";
    public static final String ERROR_TYPE_ATTRIBUTE = "error_type";

    private static final Logger logger = LogManager.getLogger(InferenceStats.class);

    private final LongCounter requestCountInstrument;
    private final LongHistogram inferenceDurationInstrument;
    private final LongHistogram deploymentDurationInstrument;
    private final Map<String, Object> constantAttributes;

    // This should be an internal constructor. It's public to enable testing
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
                "es.inference.requests.count.total",
                "Inference API request counts for a particular service, task type, model ID",
                "operations"
            ),
            meterRegistry.registerLongHistogram(
                "es.inference.requests.time",
                "Inference API request counts for a particular service, task type, model ID",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                "es.inference.trained_model.deployment.time",
                "Inference API time spent waiting for Trained Model Deployments",
                "ms"
            ),
            Map.of(STACK_VERSION_ATTRIBUTE, stackVersion, PRODUCTION_RELEASE_ATTRIBUTE, isProductionRelease)
        );
    }

    /**
     * Returns a fluent counter recording. Call {@link CounterBuilder#withSuccess()} or {@link CounterBuilder#withFailure(Throwable)}
     * to set the outcome before calling the terminal {@link CounterBuilder#incrementBy}. Omitting both records no {@code status_code},
     * which is appropriate when counting a request attempt before the outcome is known.
     */
    public CounterBuilder requestCount() {
        return new CounterBuilder(requestCountInstrument, constantAttributes);
    }

    /**
     * Returns a fluent histogram for inference request latency.
     */
    public DurationBuilder inferenceDuration() {
        return new DurationBuilder(inferenceDurationInstrument, constantAttributes);
    }

    /**
     * Returns a fluent histogram for trained model deployment wait time.
     */
    public DurationBuilder deploymentDuration() {
        return new DurationBuilder(deploymentDurationInstrument, constantAttributes);
    }

    abstract static class AbstractBuilder<B extends AbstractBuilder<B>> {
        protected final Map<String, Object> attributes;

        AbstractBuilder(Map<String, Object> constantAttributes) {
            this.attributes = new HashMap<>(constantAttributes);
        }

        @SuppressWarnings("unchecked")
        public B withModel(Model model) {
            attributes.put(SERVICE_ATTRIBUTE, model.getConfigurations().getService());
            attributes.put(TASK_TYPE_ATTRIBUTE, model.getTaskType().toString());
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B withSuccess() {
            attributes.put(STATUS_CODE_ATTRIBUTE, 200);
            return (B) this;
        }

        /**
         * Records failure attributes from the given throwable. If {@code null} is passed this behaves identically to
         * {@link #withSuccess()} and sets {@code status_code=200}. Prefer {@link #withSuccess()} when the outcome is known to be
         * successful.
         */
        @SuppressWarnings("unchecked")
        public B withFailure(@Nullable Throwable throwable) {
            if (throwable == null) {
                return withSuccess();
            }
            applyThrowable(throwable, attributes);
            return (B) this;
        }

        @SuppressWarnings("unchecked")
        public B withAttribute(String key, Object value) {
            attributes.put(key, value);
            return (B) this;
        }
    }

    /**
     * Builder for {@link LongCounter} metrics. Call {@link #withSuccess()} or {@link #withFailure(Throwable)} to set the outcome.
     * Omitting both leaves the metric without a {@code status_code} attribute, which is correct when recording a request attempt
     * before the outcome is known.
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
                logger.atDebug().withThrowable(e).log("Failed to record inference counter metric for [{}]", counter.getName());
            }
        }
    }

    /**
     * Builder for {@link LongHistogram} metrics. Call {@link #withSuccess()} or {@link #withFailure(Throwable)} to set the outcome.
     * Omitting both leaves the metric without a {@code status_code} attribute, which is correct when recording duration before the
     * outcome is known.
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
                logger.atDebug().withThrowable(e).log("Failed to record inference duration metric for [{}]", histogram.getName());
            }
        }
    }

    private static void applyThrowable(Throwable throwable, Map<String, Object> attributes) {
        if (throwable instanceof ElasticsearchStatusException ese) {
            attributes.put(STATUS_CODE_ATTRIBUTE, ese.status().getStatus());
            attributes.put(ERROR_TYPE_ATTRIBUTE, String.valueOf(ese.status().getStatus()));
        } else {
            attributes.put(ERROR_TYPE_ATTRIBUTE, throwable.getClass().getSimpleName());
        }
    }
}
