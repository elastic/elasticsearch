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
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.tasks.Task;

/**
 * Product attribution recorded on inference metrics.
 * <p>
 * Only product use case and product origin are metric attributes. The other request headers live on
 * {@link InferenceRequestMetadata} and are not available here.
 *
 * @param productUseCase the specific user flow, sourced from the {@code X-elastic-product-use-case} header (e.g. "security ai assistant").
 *                       Can be null if not defined.
 * @param productOrigin the originating system, sourced from the {@code X-elastic-product-origin} header (e.g. "kibana").
 *                      Can be null if not defined.
 */
public record InferenceProductContext(@Nullable String productUseCase, @Nullable String productOrigin) {
    public static final InferenceProductContext EMPTY = new InferenceProductContext(null, null);

    /**
     * Creates an {@link InferenceProductContext} by reading the product use case and product origin headers.
     * Other attribution headers on the thread context are ignored.
     */
    public static InferenceProductContext create(ThreadContext threadContext) {
        var useCase = threadContext.getHeader(InferenceRequestMetadata.Field.PRODUCT_USE_CASE.httpHeader());
        var origin = threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);

        if (useCase == null && origin == null) {
            return EMPTY;
        }

        return new InferenceProductContext(useCase, origin);
    }
}
