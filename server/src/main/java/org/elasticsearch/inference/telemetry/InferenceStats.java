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
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.telemetry.metric.MetricAttributesUtils.normalizeProductOrigin;

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

    public static final String SEMANTIC_TEXT_USE_CASE = "semantic_text_bulk";

    /**
     * Kibana constructs use-case strings like {@code siem_migrations_<migrationType>} where {@code migrationType} is a free-form
     * discriminator. To keep cardinality at 1 for this family, any value beginning with this prefix is collapsed to
     * {@code "siem_migrations"} before the allowlist check.
     */
    static final String SIEM_MIGRATIONS_PREFIX = "siem_migrations_";
    static final String SIEM_MIGRATIONS = "siem_migrations";

    /**
     * Sentinel value recorded for {@link #INFERENCE_SOURCE_ATTRIBUTE} and {@link MetricAttributes#ES_PRODUCT_ORIGIN}
     * when the supplied value is not in the corresponding allowlist. Bounds APM attribute cardinality.
     */
    static final String OTHER_VALUE = "other";

    static final String SECURITY_AI_ASSISTANT_USE_CASE = "security_ai_assistant";

    /**
     * Allowlist of values recorded for {@link #INFERENCE_SOURCE_ATTRIBUTE}. Any value not in this set (after lowercasing and the
     * {@link #SIEM_MIGRATIONS_PREFIX} collapse) is recorded as {@link #OTHER_VALUE}. Entries are sourced from Elasticsearch's own
     * callers (e.g. {@link #SEMANTIC_TEXT_USE_CASE}) and from Kibana plugin IDs that travel in
     * the {@code X-Elastic-Product-Use-Case} header.
     * <b>Internal callers must add their constant here and to the enforcement test. Entries must be lowercase.</b>
     * <p>
     * The telemetry plugin within kibana sets the {@code X-Elastic-Product-Use-Case} header to the plugin ID
     * (e.g. "security_ai_assistant"). The logic for this lives
     * <a href="https://github.com/elastic/kibana/blob/main/x-pack/platform/plugins/shared/inference/server/chat_complete/utils/inference_endpoint_executor.ts#L47">here</a>
     * and <a href="https://github.com/elastic/kibana/blob/main/x-pack/platform/plugins/shared/stack_connectors/server/connector_types/inference/inference.ts#L216">here</a>.
     */
    @SuppressWarnings("checkstyle:LineLength")
    public static final Set<String> KNOWN_PRODUCT_USE_CASES = Set.of(
        SEMANTIC_TEXT_USE_CASE,
        // Default if no id is specified
        // https://github.com/elastic/kibana/blob/main/x-pack/platform/plugins/shared/inference/server/chat_complete/utils/inference_endpoint_executor.ts#L47
        "inference",
        // https://github.com/elastic/kibana/blob/main/x-pack/solutions/security/plugins/elastic_assistant/server/routes/evaluate/post_evaluate.ts#L330
        SECURITY_AI_ASSISTANT_USE_CASE,
        // https://github.com/elastic/kibana/blob/main/x-pack/platform/plugins/shared/observability_ai_assistant/server/service/client/index.ts#L557
        "observability_ai_assistant",
        // https://github.com/elastic/kibana/blob/main/x-pack/solutions/security/plugins/security_solution/server/lib/siem_migrations/common/task/util/constants.ts#L8
        SIEM_MIGRATIONS,
        // https://github.com/elastic/kibana/blob/main/.buildkite/scripts/steps/evals/run_suite.sh#L17C31-L19
        "kbn_evals"
    );

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
            Map.of(MetricAttributes.ES_STACK_VERSION, stackVersion, MetricAttributes.ES_PRODUCTION_RELEASE, isProductionRelease)
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

        /**
         * Adds product attribution attributes from the given context. Values are normalized against
         * {@link #KNOWN_PRODUCT_USE_CASES} and {@link MetricAttributes#KNOWN_PRODUCT_ORIGINS};
         * anything not in the allowlist is recorded as {@link #OTHER_VALUE} to bound APM attribute cardinality.
         * Either field may be absent; they are recorded independently.
         */
        public B withProductContext(@Nullable InferenceProductContext ctx) {
            if (ctx == null) {
                return cast();
            }

            withProductUseCase(ctx.productUseCase());

            if (Strings.isNullOrEmpty(ctx.productOrigin()) == false) {
                attributes.put(MetricAttributes.ES_PRODUCT_ORIGIN, normalizeProductOrigin(ctx.productOrigin(), OTHER_VALUE));
            }

            return cast();
        }

        /**
         * Sets the {@code inference_source} attribute to the given value, normalized against {@link #KNOWN_PRODUCT_USE_CASES}.
         * Use this when the product use case is known statically at the call site (e.g. {@code "semantic_text_bulk"}). Values not in
         * the allowlist are recorded as {@link #OTHER_VALUE}; internal callers must add their constant to the allowlist.
         */
        public B withProductUseCase(String value) {
            if (Strings.isNullOrEmpty(value) == false) {
                attributes.put(INFERENCE_SOURCE_ATTRIBUTE, normalizeProductUseCase(value));
            }

            return cast();
        }

        private static String normalizeProductUseCase(String value) {
            var lowered = value.toLowerCase(Locale.ROOT);
            if (lowered.startsWith(SIEM_MIGRATIONS_PREFIX)) {
                return SIEM_MIGRATIONS;
            }
            return KNOWN_PRODUCT_USE_CASES.contains(lowered) ? lowered : OTHER_VALUE;
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
