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
     * Returns a fluent counter recording. The terminal {@link CounterBuilder#incrementBy} applies default HTTP success attributes
     * when no error was set.
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

    /**
     * Builder for {@link LongCounter} metrics. {@link #withThrowable(Throwable)} is optional; omit it to indicate success.
     */
    public static class CounterBuilder {
        private final LongCounter counter;
        private final Map<String, Object> attributes;
        private boolean responseSet = false;

        CounterBuilder(LongCounter counter, Map<String, Object> constantAttributes) {
            this.counter = counter;
            this.attributes = new HashMap<>(constantAttributes);
        }

        public CounterBuilder withModel(Model model) {
            attributes.put(SERVICE_ATTRIBUTE, model.getConfigurations().getService());
            attributes.put(TASK_TYPE_ATTRIBUTE, model.getTaskType().toString());
            return this;
        }

        public CounterBuilder withThrowable(@Nullable Throwable t) {
            if (applyThrowable(t, attributes)) {
                responseSet = true;
            }
            return this;
        }

        public CounterBuilder withAttribute(String key, Object value) {
            attributes.put(key, value);
            return this;
        }

        public void incrementBy(long value) {
            if (responseSet == false) {
                attributes.put(STATUS_CODE_ATTRIBUTE, 200);
            }

            try {
                counter.incrementBy(value, attributes);
            } catch (Exception e) {
                logger.atDebug().withThrowable(e).log("Failed to record inference counter metric for [{}]", counter.getName());
            }
        }
    }

    /**
     * Builder for {@link LongHistogram} metrics. {@link #withThrowable(Throwable)} is optional; omit it for success.
     */
    public static class DurationBuilder {
        private final LongHistogram histogram;
        private final Map<String, Object> attributes;
        private boolean responseSet = false;

        DurationBuilder(LongHistogram histogram, Map<String, Object> constantAttributes) {
            this.histogram = histogram;
            this.attributes = new HashMap<>(constantAttributes);
        }

        public DurationBuilder withModel(Model model) {
            attributes.put(SERVICE_ATTRIBUTE, model.getConfigurations().getService());
            attributes.put(TASK_TYPE_ATTRIBUTE, model.getTaskType().toString());
            return this;
        }

        public DurationBuilder withThrowable(@Nullable Throwable t) {
            if (applyThrowable(t, attributes)) {
                responseSet = true;
            }
            return this;
        }

        public void record(long durationMs) {
            if (responseSet == false) {
                attributes.put(STATUS_CODE_ATTRIBUTE, 200);
            }

            try {
                histogram.record(durationMs, attributes);
            } catch (Exception e) {
                logger.atDebug().withThrowable(e).log("Failed to record inference duration metric for [{}]", histogram.getName());
            }
        }
    }

    private static boolean applyThrowable(@Nullable Throwable throwable, Map<String, Object> attributes) {
        if (throwable == null) {
            return false;
        }

        if (throwable instanceof ElasticsearchStatusException ese) {
            attributes.put(STATUS_CODE_ATTRIBUTE, ese.status().getStatus());
            attributes.put(ERROR_TYPE_ATTRIBUTE, String.valueOf(ese.status().getStatus()));
        } else {
            attributes.put(ERROR_TYPE_ATTRIBUTE, throwable.getClass().getSimpleName());
        }

        return true;
    }
}
