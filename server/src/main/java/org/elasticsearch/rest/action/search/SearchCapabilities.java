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
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
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
    /** Support regex and range match rules in interval queries. */
    private static final String KQL_QUERY_SUPPORTED = "kql_query";
    public static final Set<String> CAPABILITIES = capabilities();

    private static Set<String> capabilities() {
        Set<String> capabilities = Set.of(
            RANGE_REGEX_INTERVAL_QUERY_CAPABILITY,
            BIT_DENSE_VECTOR_SYNTHETIC_SOURCE_CAPABILITY
        );

        if (Build.current().isSnapshot()) {
            return Collections.unmodifiableSet(Sets.union(capabilities, snapshotBuildCapabilities()));
        }

        return capabilities;
    }

    private static Set<String>  snapshotBuildCapabilities() {
        return Set.of(KQL_QUERY_SUPPORTED);
    }
}
