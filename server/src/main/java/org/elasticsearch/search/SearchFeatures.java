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

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {

    public static final NodeFeature LUCENE_10_0_0_UPGRADE = new NodeFeature("lucene_10_upgrade");
    public static final NodeFeature LUCENE_10_1_0_UPGRADE = new NodeFeature("lucene_10_1_upgrade");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(LUCENE_10_0_0_UPGRADE, LUCENE_10_1_0_UPGRADE);
    }

    public static final NodeFeature RETRIEVER_RESCORER_ENABLED = new NodeFeature("search.retriever.rescorer.enabled");
    public static final NodeFeature COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS = new NodeFeature(
        "search.completion_field.duplicate.support"
    );
    public static final NodeFeature RESCORER_MISSING_FIELD_BAD_REQUEST = new NodeFeature("search.rescorer.missing.field.bad.request");
    public static final NodeFeature INT_SORT_FOR_INT_SHORT_BYTE_FIELDS = new NodeFeature("search.sort.int_sort_for_int_short_byte_fields");
    static final NodeFeature MULTI_MATCH_CHECKS_POSITIONS = new NodeFeature("search.multi.match.checks.positions");
    public static final NodeFeature BBQ_HNSW_DEFAULT_INDEXING = new NodeFeature("search.vectors.mappers.default_bbq_hnsw");
    public static final NodeFeature SEARCH_WITH_NO_DIMENSIONS_BUGFIX = new NodeFeature("search.vectors.no_dimensions_bugfix");
    public static final NodeFeature SEARCH_RESCORE_SCRIPT = new NodeFeature("search.rescore.script");
    public static final NodeFeature NEGATIVE_FUNCTION_SCORE_BAD_REQUEST = new NodeFeature("search.negative.function.score.bad.request");
    public static final NodeFeature INDICES_BOOST_REMOTE_INDEX_FIX = new NodeFeature("search.indices_boost_remote_index_fix");
    public static final NodeFeature NESTED_AGG_TOP_HITS_WITH_INNER_HITS = new NodeFeature("nested_agg_top_hits_with_inner_hits");
    public static final NodeFeature DATE_FORMAT_MISSING_AS_NULL = new NodeFeature("search.sort.date_format_missing_as_null");
    public static final NodeFeature LIMIT_MAX_IDS_FEATURE = new NodeFeature("ids_query_limit_max_ids");
    public static final NodeFeature EXPONENTIAL_HISTOGRAM_QUERYDSL_MIN_MAX = new NodeFeature(
        "search.exponential_histogram_querydsl_min_max"
    );
    public static final NodeFeature FUNCTION_SCORE_NAMED_QUERIES = new NodeFeature("search.function_score.named_queries");
    public static final NodeFeature EXPONENTIAL_HISTOGRAM_QUERYDSL_PERCENTILES = new NodeFeature(
        "search.exponential_histogram_querydsl_percentiles"
    );
    public static final NodeFeature EXPONENTIAL_HISTOGRAM_QUERYDSL_PERCENTILE_RANKS = new NodeFeature(
        "search.exponential_histogram_querydsl_percentile_ranks"
    );
    public static final NodeFeature CLOSING_INVALID_PIT_ID = new NodeFeature("closing_invalid_pit_id");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            RETRIEVER_RESCORER_ENABLED,
            COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS,
            RESCORER_MISSING_FIELD_BAD_REQUEST,
            INT_SORT_FOR_INT_SHORT_BYTE_FIELDS,
            MULTI_MATCH_CHECKS_POSITIONS,
            BBQ_HNSW_DEFAULT_INDEXING,
            SEARCH_WITH_NO_DIMENSIONS_BUGFIX,
            SEARCH_RESCORE_SCRIPT,
            NEGATIVE_FUNCTION_SCORE_BAD_REQUEST,
            INDICES_BOOST_REMOTE_INDEX_FIX,
            NESTED_AGG_TOP_HITS_WITH_INNER_HITS,
            DATE_FORMAT_MISSING_AS_NULL,
            LIMIT_MAX_IDS_FEATURE,
            EXPONENTIAL_HISTOGRAM_QUERYDSL_MIN_MAX,
            EXPONENTIAL_HISTOGRAM_QUERYDSL_PERCENTILES,
            EXPONENTIAL_HISTOGRAM_QUERYDSL_PERCENTILE_RANKS,
            CLOSING_INVALID_PIT_ID,
            FUNCTION_SCORE_NAMED_QUERIES
        );
    }
}
