/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import java.util.Set;

/**
 * A {@link Set} of "capabilities" supported by the {@link RestSearchAction}.
 */
public final class SearchCapabilities {

    private SearchCapabilities() {}

    /** Support regex and range match rules in interval queries. */
    private static final String RANGE_REGEX_INTERVAL_QUERY_CAPABILITY = "range_regexp_interval_queries";
    /** Support synthetic source with `bit` type in `dense_vector` field when `index` is set to `false`. */
    private static final String BIT_DENSE_VECTOR_SYNTHETIC_SOURCE_CAPABILITY = "bit_dense_vector_synthetic_source";
    private static final String NESTED_RETRIEVER_INNER_HITS_SUPPORT = "nested_retriever_inner_hits_support";
    private static final String RANDOM_SAMPLER_WITH_SCORED_SUBAGGS = "random_sampler_with_scored_subaggs";
    /** Support deprecated window_size field in rank. */
    private static final String RRF_WINDOW_SIZE_SUPPORT_DEPRECATED = "rrf_window_size_support_deprecated";

    public static final Set<String> CAPABILITIES = Set.of(
        RANGE_REGEX_INTERVAL_QUERY_CAPABILITY,
        BIT_DENSE_VECTOR_SYNTHETIC_SOURCE_CAPABILITY,
        NESTED_RETRIEVER_INNER_HITS_SUPPORT,
        RANDOM_SAMPLER_WITH_SCORED_SUBAGGS,
        RRF_WINDOW_SIZE_SUPPORT_DEPRECATED
    );
}
