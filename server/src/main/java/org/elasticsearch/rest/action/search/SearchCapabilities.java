/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

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
    /** Support multi-dense-vector field mapper. */
    private static final String MULTI_DENSE_VECTOR_FIELD_MAPPER = "multi_dense_vector_field_mapper";

    public static final Set<String> CAPABILITIES;
    static {
        HashSet<String> capabilities = new HashSet<>();
        capabilities.add(RANGE_REGEX_INTERVAL_QUERY_CAPABILITY);
        capabilities.add(BIT_DENSE_VECTOR_SYNTHETIC_SOURCE_CAPABILITY);
        if (MultiDenseVectorFieldMapper.FEATURE_FLAG.isEnabled()) {
            capabilities.add(MULTI_DENSE_VECTOR_FIELD_MAPPER);
        }
        CAPABILITIES = Set.copyOf(capabilities);
    }
}
