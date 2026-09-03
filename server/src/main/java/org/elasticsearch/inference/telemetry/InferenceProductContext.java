/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

import java.util.List;

/**
 * Carries per-request product attribution context.
 *
 * @param productUseCase the specific user flow, sourced from the {@code X-elastic-product-use-case} header (e.g. "security ai assistant").
 *                       Can be null if not defined.
 * @param productOrigin the originating system, sourced from the {@code X-elastic-product-origin} header (e.g. "kibana").
 *                      Can be null if not defined.
 * @param productSolution the originating Elastic solution, sourced from the {@code X-elastic-product-solution} header
 *                        (e.g. "security"). Can be null if not defined.
 * @param productFeature the stable inference feature identifier, sourced from the {@code X-elastic-product-feature} header
 *                       (e.g. "attack_discovery"). Can be null if not defined.
 * @param interactionId an identifier used to attribute related inference requests, sourced from the
 *                      {@code X-Elastic-Inference-Interaction-Id} header. Can be null if not defined.
 */
public record InferenceProductContext(
    @Nullable String productUseCase,
    @Nullable String productOrigin,
    @Nullable String productSolution,
    @Nullable String productFeature,
    @Nullable String interactionId
) {
    public static final String X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER = "X-elastic-product-use-case";
    public static final String X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER = "X-elastic-product-solution";
    public static final String X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER = "X-elastic-product-feature";
    public static final String X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER = "X-Elastic-Inference-Interaction-Id";

    /**
     * The inference-specific attribution headers. {@code X-elastic-product-origin} is deliberately absent: it is core-owned
     * ({@link Task#X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER}) and registered by the server rather than by the inference plugin.
     */
    public static final List<String> ATTRIBUTION_HEADERS = List.of(
        X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER,
        X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER,
        X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER,
        X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER
    );

    public static final InferenceProductContext EMPTY = new InferenceProductContext(null, null, null, null, null);

    public InferenceProductContext(@Nullable String productUseCase, @Nullable String productOrigin) {
        this(productUseCase, productOrigin, null, null, null);
    }

    /**
     * Creates an {@link InferenceProductContext} by reading the product attribution headers from the given thread context.
     */
    public static InferenceProductContext create(ThreadContext threadContext) {
        var useCase = threadContext.getHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
        var origin = threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);
        var solution = threadContext.getHeader(X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER);
        var feature = threadContext.getHeader(X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER);
        var interactionId = threadContext.getHeader(X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER);

        if (useCase == null && origin == null && solution == null && feature == null && interactionId == null) {
            return EMPTY;
        }

        return new InferenceProductContext(useCase, origin, solution, feature, interactionId);
    }
}
