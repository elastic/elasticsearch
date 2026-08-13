/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(KnnVectorQueryBuilder.K_PARAM_SUPPORTED);
    }

    public static final NodeFeature RETRIEVER_RESCORER_ENABLED = new NodeFeature("search.retriever.rescorer.enabled");
    public static final NodeFeature COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS = new NodeFeature(
        "search.completion_field.duplicate.support"
    );
    public static final NodeFeature RESCORER_MISSING_FIELD_BAD_REQUEST = new NodeFeature("search.rescorer.missing.field.bad.request");
    public static final NodeFeature INT_SORT_FOR_INT_SHORT_BYTE_FIELDS = new NodeFeature("search.sort.int_sort_for_int_short_byte_fields");
    static final NodeFeature MULTI_MATCH_CHECKS_POSITIONS = new NodeFeature("search.multi.match.checks.positions");
    private static final NodeFeature KNN_QUERY_BUGFIX_130254 = new NodeFeature("search.knn.query.bugfix.130254", true);
    public static final NodeFeature SEARCH_WITH_NO_DIMENSIONS_BUGFIX = new NodeFeature("search.vectors.no_dimensions_bugfix");
    public static final NodeFeature DATE_FORMAT_MISSING_AS_NULL = new NodeFeature("search.sort.date_format_missing_as_null");
    /**
     * A non-top-level {@code date_histogram} with {@code hard_bounds} that excludes every fixed rounding point
     * produced from the data no longer throws {@code ArrayIndexOutOfBoundsException}; it returns an empty histogram.
     */
    public static final NodeFeature DATE_HISTOGRAM_HARD_BOUNDS_OUTSIDE_DATA_FIX = new NodeFeature(
        "search.aggs.date_histogram.hard_bounds_outside_data_fix"
    );
    /**
     * Test-only gate for REST tests asserting that a user-mapped field named {@code _type} is not
     * surfaced as root-level hit metadata. Old nodes included it in the default metadata fetch
     * regardless of whether it was a real metadata mapper.
     */
    public static final NodeFeature FETCH_FIELDS_EXCLUDES_NON_METADATA_TYPE = new NodeFeature(
        "search.fetch_fields.excludes_non_metadata_type"
    );
    /**
     * Test-only gate for REST tests asserting that {@code inner_hits} of a nested kNN query score with the same
     * fidelity the query phase used. Older nodes score the fetch phase against the quantized vectors while the query
     * phase rescores against the full-precision ones, so those tests cannot pass on a mixed BWC cluster.
     */
    public static final NodeFeature NESTED_KNN_INNER_HITS_MATCH_QUERY_PHASE_SCORING = new NodeFeature(
        "search.vectors.nested_knn_inner_hits_match_query_phase_scoring"
    );

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            RETRIEVER_RESCORER_ENABLED,
            COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS,
            INT_SORT_FOR_INT_SHORT_BYTE_FIELDS,
            MULTI_MATCH_CHECKS_POSITIONS,
            KNN_QUERY_BUGFIX_130254,
            SEARCH_WITH_NO_DIMENSIONS_BUGFIX,
            DATE_FORMAT_MISSING_AS_NULL,
            DATE_HISTOGRAM_HARD_BOUNDS_OUTSIDE_DATA_FIX,
            FETCH_FIELDS_EXCLUDES_NON_METADATA_TYPE,
            NESTED_KNN_INNER_HITS_MATCH_QUERY_PHASE_SCORING
        );
    }
}
