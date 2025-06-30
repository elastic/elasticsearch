/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.Build;
import org.elasticsearch.index.mapper.vectors.MultiDenseVectorFieldMapper;

import java.util.HashSet;
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
    /** Support Byte and Float with Bit dot product. */
    private static final String BYTE_FLOAT_BIT_DOT_PRODUCT_CAPABILITY = "byte_float_bit_dot_product_with_bugfix";
    /** Support docvalue_fields parameter for `dense_vector` field. */
    private static final String DENSE_VECTOR_DOCVALUE_FIELDS = "dense_vector_docvalue_fields";
    /** Support kql query. */
    private static final String KQL_QUERY_SUPPORTED = "kql_query";
    /** Support multi-dense-vector field mapper. */
    private static final String MULTI_DENSE_VECTOR_FIELD_MAPPER = "multi_dense_vector_field_mapper";
    /** Support propagating nested retrievers' inner_hits to top-level compound retrievers . */
    private static final String NESTED_RETRIEVER_INNER_HITS_SUPPORT = "nested_retriever_inner_hits_support";
    /** Support multi-dense-vector script field access. */
    private static final String MULTI_DENSE_VECTOR_SCRIPT_ACCESS = "multi_dense_vector_script_access";
    /** Fixed the math in {@code moving_fn}'s {@code linearWeightedAvg}. */
    private static final String MOVING_FN_RIGHT_MATH = "moving_fn_right_math";

    private static final String RANDOM_SAMPLER_WITH_SCORED_SUBAGGS = "random_sampler_with_scored_subaggs";
    /** Support deprecated window_size field in rank. */
    private static final String RRF_WINDOW_SIZE_SUPPORT_DEPRECATED = "rrf_window_size_support_deprecated";

    private static final String SIGNIFICANT_TERMS_ON_NESTED_FIELDS = "significant_terms_on_nested_fields";

    public static final Set<String> CAPABILITIES;
    static {
        HashSet<String> capabilities = new HashSet<>();
        capabilities.add(RANGE_REGEX_INTERVAL_QUERY_CAPABILITY);
        capabilities.add(BIT_DENSE_VECTOR_SYNTHETIC_SOURCE_CAPABILITY);
        capabilities.add(BYTE_FLOAT_BIT_DOT_PRODUCT_CAPABILITY);
        capabilities.add(DENSE_VECTOR_DOCVALUE_FIELDS);
        capabilities.add(NESTED_RETRIEVER_INNER_HITS_SUPPORT);
        capabilities.add(RANDOM_SAMPLER_WITH_SCORED_SUBAGGS);
        capabilities.add(MOVING_FN_RIGHT_MATH);
        if (MultiDenseVectorFieldMapper.FEATURE_FLAG.isEnabled()) {
            capabilities.add(MULTI_DENSE_VECTOR_FIELD_MAPPER);
            capabilities.add(MULTI_DENSE_VECTOR_SCRIPT_ACCESS);
        }
        if (Build.current().isSnapshot()) {
            capabilities.add(KQL_QUERY_SUPPORTED);
        }
        capabilities.add(RRF_WINDOW_SIZE_SUPPORT_DEPRECATED);
        capabilities.add(SIGNIFICANT_TERMS_ON_NESTED_FIELDS);
        CAPABILITIES = Set.copyOf(capabilities);
    }
}
