/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to introspect a search request and extract metadata from it around the features it uses.
 */
public class SearchRequestIntrospector {

    /**
     * Introspects the provided search request and extracts metadata from it about some of its characteristics.
     *
     */
    public static QueryMetadata introspectSearchRequest(SearchRequest searchRequest) {

        String target = introspectIndices(searchRequest.indices());

        String pitOrScroll = null;
        if (searchRequest.scroll() != null) {
            pitOrScroll = SCROLL;
        }

        SearchSourceBuilder searchSourceBuilder = searchRequest.source();
        if (searchSourceBuilder == null) {
            return new QueryMetadata(target, ScoreSortBuilder.NAME, HITS_ONLY, false, Strings.EMPTY_ARRAY, pitOrScroll);
        }

        if (searchSourceBuilder.pointInTimeBuilder() != null) {
            pitOrScroll = PIT;
        }

        final String primarySort;
        if (searchSourceBuilder.sorts() == null || searchSourceBuilder.sorts().isEmpty()) {
            primarySort = ScoreSortBuilder.NAME;
        } else {
            primarySort = introspectPrimarySort(searchSourceBuilder.sorts().getFirst());
        }

        final String queryType = introspectQueryType(searchSourceBuilder);

        QueryMetadataBuilder queryMetadataBuilder = new QueryMetadataBuilder();
        if (searchSourceBuilder.query() != null) {
            introspectQueryBuilder(searchSourceBuilder.query(), queryMetadataBuilder);
        }

        final boolean hasKnn = searchSourceBuilder.knnSearch().isEmpty() == false || queryMetadataBuilder.knnQuery;

        return new QueryMetadata(
            target,
            primarySort,
            queryType,
            hasKnn,
            queryMetadataBuilder.rangeFields.toArray(new String[0]),
            pitOrScroll
        );
    }

    private static final class QueryMetadataBuilder {
        private boolean knnQuery = false;
        private final List<String> rangeFields = new ArrayList<>();
    }

    public record QueryMetadata(
        String target,
        String primarySort,
        String queryType,
        boolean knn,
        String[] rangeFields,
        String pitOrScroll
    ) {

        public Map<String, Object> toAttributesMap() {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(TARGET_ATTRIBUTE, target);
            attributes.put(SORT_ATTRIBUTE, primarySort);
            if (pitOrScroll == null) {
                attributes.put(QUERY_TYPE_ATTRIBUTE, queryType);
            } else {
                attributes.put(QUERY_TYPE_ATTRIBUTE, Arrays.asList(queryType, pitOrScroll));
            }

            attributes.put(KNN_ATTRIBUTE, knn);
            attributes.put(RANGES_ATTRIBUTE, rangeFields);
            return attributes;
        }
    }

    private static final String TARGET_ATTRIBUTE = "target";
    private static final String SORT_ATTRIBUTE = "sort";
    private static final String QUERY_TYPE_ATTRIBUTE = "query_type";
    private static final String KNN_ATTRIBUTE = "knn";
    private static final String RANGES_ATTRIBUTE = "ranges";

    private static final String TARGET_KIBANA = ".kibana";
    private static final String TARGET_ML = ".ml";
    private static final String TARGET_FLEET = ".fleet";
    private static final String TARGET_SLO = ".slo";
    private static final String TARGET_ALERTS = ".alerts";
    private static final String TARGET_ELASTIC = ".elastic";
    private static final String TARGET_DS = ".ds-";
    private static final String TARGET_OTHERS = ".others";
    private static final String TARGET_USER = "user";

    static String introspectIndices(String[] indices) {
        // Note that indices are expected to be resolved, meaning wildcards are not handled on purpose
        // If indices resolve to data streams, the name of the data stream is returned as opposed to its backing indices
        if (indices.length == 1) {
            String index = indices[0];
            if (index.startsWith(".")) {
                if (index.startsWith(TARGET_KIBANA)) {
                    return TARGET_KIBANA;
                }
                if (index.startsWith(TARGET_ML)) {
                    return TARGET_ML;
                }
                if (index.startsWith(TARGET_FLEET)) {
                    return TARGET_FLEET;
                }
                if (index.startsWith(TARGET_SLO)) {
                    return TARGET_SLO;
                }
                if (index.startsWith(TARGET_ALERTS)) {
                    return TARGET_ALERTS;
                }
                if (index.startsWith(TARGET_ELASTIC)) {
                    return TARGET_ELASTIC;
                }
                // this happens only when data streams backing indices are searched explicitly
                if (index.startsWith(TARGET_DS)) {
                    return TARGET_DS;
                }
                return TARGET_OTHERS;
            }
        }
        return TARGET_USER;
    }

    private static final String TIMESTAMP = "@timestamp";
    private static final String EVENT_INGESTED = "event.ingested";
    private static final String _DOC = "_doc";
    private static final String FIELD = "field";

    static String introspectPrimarySort(SortBuilder<?> primarySortBuilder) {
        if (primarySortBuilder instanceof FieldSortBuilder fieldSort) {
            return switch (fieldSort.getFieldName()) {
                case TIMESTAMP -> TIMESTAMP;
                case EVENT_INGESTED -> EVENT_INGESTED;
                case _DOC -> _DOC;
                default -> FIELD;
            };
        }
        return primarySortBuilder.getWriteableName();
    }

    private static final String HITS_AND_AGGS = "hits_and_aggs";
    private static final String HITS_ONLY = "hits_only";
    private static final String AGGS_ONLY = "aggs_only";
    private static final String COUNT_ONLY = "count_only";
    private static final String PIT = "pit";
    private static final String SCROLL = "scroll";

    public static final Map<String, Object> SEARCH_SCROLL_ATTRIBUTES = Map.of(QUERY_TYPE_ATTRIBUTE, SCROLL);

    static String introspectQueryType(SearchSourceBuilder searchSourceBuilder) {
        int size = searchSourceBuilder.size();
        if (size == -1) {
            size = SearchService.DEFAULT_SIZE;
        }
        if (searchSourceBuilder.aggregations() != null && size > 0) {
            return HITS_AND_AGGS;
        }
        if (searchSourceBuilder.aggregations() != null) {
            return AGGS_ONLY;
        }
        if (size > 0) {
            return HITS_ONLY;
        }
        return COUNT_ONLY;
    }

    static void introspectQueryBuilder(QueryBuilder queryBuilder, QueryMetadataBuilder queryMetadataBuilder) {
        switch (queryBuilder) {
            case BoolQueryBuilder bool:
                for (QueryBuilder must : bool.must()) {
                    introspectQueryBuilder(must, queryMetadataBuilder);
                }
                for (QueryBuilder filter : bool.filter()) {
                    introspectQueryBuilder(filter, queryMetadataBuilder);
                }
                if (bool.must().isEmpty() && bool.filter().isEmpty() && bool.mustNot().isEmpty() && bool.should().size() == 1) {
                    introspectQueryBuilder(bool.should().getFirst(), queryMetadataBuilder);
                }
                // Note that should clauses are ignored unless there's only one that becomes mandatory
                // must_not clauses are also ignored for now
                break;
            case ConstantScoreQueryBuilder constantScore:
                introspectQueryBuilder(constantScore.innerQuery(), queryMetadataBuilder);
                break;
            case BoostingQueryBuilder boosting:
                introspectQueryBuilder(boosting.positiveQuery(), queryMetadataBuilder);
                break;
            case NestedQueryBuilder nested:
                introspectQueryBuilder(nested.query(), queryMetadataBuilder);
                break;
            case RangeQueryBuilder range:
                switch (range.fieldName()) {
                    case TIMESTAMP -> queryMetadataBuilder.rangeFields.add(TIMESTAMP);
                    case EVENT_INGESTED -> queryMetadataBuilder.rangeFields.add(EVENT_INGESTED);
                    default -> queryMetadataBuilder.rangeFields.add(FIELD);
                }
                break;
            case KnnVectorQueryBuilder knn:
                queryMetadataBuilder.knnQuery = true;
                break;
            default:
        }
    }
}
