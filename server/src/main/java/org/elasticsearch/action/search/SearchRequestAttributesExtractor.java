/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to introspect a search request and extract metadata from it around the features it uses.
 * Given that the purpose of this class is to extract metrics attributes, it should do its best
 * to extract the minimum set of needed information without hurting performance, and without
 * ever breaking: if something goes wrong around extracting attributes, it should skip extracting
 * them as opposed to failing the search.
 */
public class SearchRequestAttributesExtractor {
    private static final Logger logger = LogManager.getLogger(SearchRequestAttributesExtractor.class);

    /**
     * Introspects the provided search request and extracts metadata from it about some of its characteristics.
     *
     */
    public static Map<String, Object> extractAttributes(SearchRequest searchRequest) {
        String target = extractIndices(searchRequest.indices());

        String pitOrScroll = null;
        if (searchRequest.scroll() != null) {
            pitOrScroll = SCROLL;
        }

        SearchSourceBuilder searchSourceBuilder = searchRequest.source();
        if (searchSourceBuilder == null) {
            return buildAttributesMap(target, ScoreSortBuilder.NAME, HITS_ONLY, false, null, pitOrScroll);
        }

        if (searchSourceBuilder.pointInTimeBuilder() != null) {
            pitOrScroll = PIT;
        }

        final String primarySort;
        if (searchSourceBuilder.sorts() == null || searchSourceBuilder.sorts().isEmpty()) {
            primarySort = ScoreSortBuilder.NAME;
        } else {
            primarySort = extractPrimarySort(searchSourceBuilder.sorts().getFirst());
        }

        final String queryType = extractQueryType(searchSourceBuilder);

        QueryMetadataBuilder queryMetadataBuilder = new QueryMetadataBuilder();
        if (searchSourceBuilder.query() != null) {
            try {
                introspectQueryBuilder(searchSourceBuilder.query(), queryMetadataBuilder);
            } catch (Exception e) {
                logger.error("Failed to extract query attribute", e);
            }
        }

        final boolean hasKnn = searchSourceBuilder.knnSearch().isEmpty() == false || queryMetadataBuilder.knnQuery;
        return buildAttributesMap(target, primarySort, queryType, hasKnn, queryMetadataBuilder.getRangeFields(), pitOrScroll);
    }

    private static Map<String, Object> buildAttributesMap(
        String target,
        String primarySort,
        String queryType,
        boolean knn,
        String[] rangeFields,
        String pitOrScroll
    ) {
        Map<String, Object> attributes = new HashMap<>(5, 1.0f);
        attributes.put(TARGET_ATTRIBUTE, target);
        attributes.put(SORT_ATTRIBUTE, primarySort);
        if (pitOrScroll == null) {
            attributes.put(QUERY_TYPE_ATTRIBUTE, queryType);
        } else {
            attributes.put(QUERY_TYPE_ATTRIBUTE, new String[] { queryType, pitOrScroll });
        }

        attributes.put(KNN_ATTRIBUTE, knn);
        if (rangeFields != null) {
            attributes.put(RANGES_ATTRIBUTE, rangeFields);
        }
        return attributes;
    }

    private static final class QueryMetadataBuilder {
        private boolean knnQuery = false;
        private final List<String> rangeFields = new ArrayList<>();

        String[] getRangeFields() {
            if (rangeFields.isEmpty()) {
                return null;
            }
            return rangeFields.toArray(new String[0]);
        }
    }

    static final String TARGET_ATTRIBUTE = "target";
    static final String SORT_ATTRIBUTE = "sort";
    static final String QUERY_TYPE_ATTRIBUTE = "query_type";
    static final String KNN_ATTRIBUTE = "knn";
    static final String RANGES_ATTRIBUTE = "ranges";

    private static final String TARGET_KIBANA = ".kibana";
    private static final String TARGET_ML = ".ml";
    private static final String TARGET_FLEET = ".fleet";
    private static final String TARGET_SLO = ".slo";
    private static final String TARGET_ALERTS = ".alerts";
    private static final String TARGET_ELASTIC = ".elastic";
    private static final String TARGET_DS = ".ds-";
    private static final String TARGET_OTHERS = ".others";
    private static final String TARGET_USER = "user";
    private static final String ERROR = "error";

    static String extractIndices(String[] indices) {
        try {
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
        } catch (Exception e) {
            logger.error("Failed to extract indices attribute", e);
            return ERROR;
        }
    }

    private static final String TIMESTAMP = "@timestamp";
    private static final String EVENT_INGESTED = "event.ingested";
    private static final String _DOC = "_doc";
    private static final String FIELD = "field";

    static String extractPrimarySort(SortBuilder<?> primarySortBuilder) {
        try {
            if (primarySortBuilder instanceof FieldSortBuilder fieldSort) {
                return switch (fieldSort.getFieldName()) {
                    case TIMESTAMP -> TIMESTAMP;
                    case EVENT_INGESTED -> EVENT_INGESTED;
                    case _DOC -> _DOC;
                    default -> FIELD;
                };
            }
            return primarySortBuilder.getWriteableName();
        } catch (Exception e) {
            logger.error("Failed to extract primary sort attribute", e);
            return ERROR;
        }
    }

    private static final String HITS_AND_AGGS = "hits_and_aggs";
    private static final String HITS_ONLY = "hits_only";
    private static final String AGGS_ONLY = "aggs_only";
    private static final String COUNT_ONLY = "count_only";
    private static final String PIT = "pit";
    private static final String SCROLL = "scroll";

    public static final Map<String, Object> SEARCH_SCROLL_ATTRIBUTES = Map.of(QUERY_TYPE_ATTRIBUTE, SCROLL);

    static String extractQueryType(SearchSourceBuilder searchSourceBuilder) {
        try {
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
        } catch (Exception e) {
            logger.error("Failed to extract query type attribute", e);
            return ERROR;
        }
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
