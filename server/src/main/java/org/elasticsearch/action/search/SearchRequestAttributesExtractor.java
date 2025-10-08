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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to introspect a search request and extract metadata from it around the features it uses.
 * Given that the purpose of this class is to extract metrics attributes, it should do its best
 * to extract the minimum set of needed information without hurting performance, and without
 * ever breaking: if something goes wrong around extracting attributes, it should skip extracting
 * them as opposed to failing the search.
 */
public final class SearchRequestAttributesExtractor {
    private static final Logger logger = LogManager.getLogger(SearchRequestAttributesExtractor.class);

    private SearchRequestAttributesExtractor() {}

    /**
     * Introspects the provided search request and extracts metadata from it about some of its characteristics.
     */
    public static Map<String, Object> extractAttributes(SearchRequest searchRequest, String[] localIndices) {
        return extractAttributes(searchRequest.source(), searchRequest.scroll(), null, -1, localIndices);
    }

    /**
     * Introspects the provided shard search request and extracts metadata from it about some of its characteristics.
     */
    public static Map<String, Object> extractAttributes(
        ShardSearchRequest shardSearchRequest,
        Long timeRangeFilterFromMillis,
        long nowInMillis
    ) {
        Map<String, Object> attributes = extractAttributes(
            shardSearchRequest.source(),
            shardSearchRequest.scroll(),
            timeRangeFilterFromMillis,
            nowInMillis,
            shardSearchRequest.shardId().getIndexName()
        );
        boolean isSystem = ((EsExecutors.EsThread) Thread.currentThread()).isSystem();
        attributes.put(SYSTEM_THREAD_ATTRIBUTE_NAME, isSystem);
        return attributes;
    }

    private static Map<String, Object> extractAttributes(
        SearchSourceBuilder searchSourceBuilder,
        TimeValue scroll,
        Long timeRangeFilterFromMillis,
        long nowInMillis,
        String... localIndices
    ) {
        String target = extractIndices(localIndices);

        String pitOrScroll = null;
        if (scroll != null) {
            pitOrScroll = SCROLL;
        }

        if (searchSourceBuilder == null) {
            return buildAttributesMap(target, ScoreSortBuilder.NAME, HITS_ONLY, false, false, false, pitOrScroll, null);
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
                introspectQueryBuilder(searchSourceBuilder.query(), queryMetadataBuilder, 0);
            } catch (Exception e) {
                logger.error("Failed to extract query attributes", e);
            }
        }

        if (searchSourceBuilder.retriever() != null) {
            try {
                introspectRetriever(searchSourceBuilder.retriever(), queryMetadataBuilder, 0);
            } catch (Exception e) {
                logger.error("Failed to extract retriever attributes", e);
            }
        }

        final boolean hasKnn = searchSourceBuilder.knnSearch().isEmpty() == false || queryMetadataBuilder.knnQuery;
        String timeRangeFilterFrom = null;
        if (timeRangeFilterFromMillis != null) {
            timeRangeFilterFrom = introspectTimeRange(timeRangeFilterFromMillis, nowInMillis);
        }
        return buildAttributesMap(
            target,
            primarySort,
            queryType,
            hasKnn,
            queryMetadataBuilder.rangeOnTimestamp,
            queryMetadataBuilder.rangeOnEventIngested,
            pitOrScroll,
            timeRangeFilterFrom
        );
    }

    private static Map<String, Object> buildAttributesMap(
        String target,
        String primarySort,
        String queryType,
        boolean knn,
        boolean rangeOnTimestamp,
        boolean rangeOnEventIngested,
        String pitOrScroll,
        String timeRangeFilterFrom
    ) {
        Map<String, Object> attributes = new HashMap<>(5, 1.0f);
        attributes.put(TARGET_ATTRIBUTE, target);
        attributes.put(SORT_ATTRIBUTE, primarySort);
        attributes.put(QUERY_TYPE_ATTRIBUTE, queryType);
        if (pitOrScroll != null) {
            attributes.put(PIT_SCROLL_ATTRIBUTE, pitOrScroll);
        }
        if (knn) {
            attributes.put(KNN_ATTRIBUTE, knn);
        }
        if (rangeOnTimestamp && rangeOnEventIngested) {
            attributes.put(
                TIME_RANGE_FILTER_FIELD_ATTRIBUTE,
                DataStream.TIMESTAMP_FIELD_NAME + "_AND_" + IndexMetadata.EVENT_INGESTED_FIELD_NAME
            );
        } else if (rangeOnEventIngested) {
            attributes.put(TIME_RANGE_FILTER_FIELD_ATTRIBUTE, IndexMetadata.EVENT_INGESTED_FIELD_NAME);
        } else if (rangeOnTimestamp) {
            attributes.put(TIME_RANGE_FILTER_FIELD_ATTRIBUTE, DataStream.TIMESTAMP_FIELD_NAME);
        }
        if (timeRangeFilterFrom != null) {
            attributes.put(TIME_RANGE_FILTER_FROM_ATTRIBUTE, timeRangeFilterFrom);
        }
        return attributes;
    }

    private static final class QueryMetadataBuilder {
        private boolean knnQuery = false;
        private boolean rangeOnTimestamp = false;
        private boolean rangeOnEventIngested = false;
    }

    static final String TARGET_ATTRIBUTE = "target";
    static final String SORT_ATTRIBUTE = "sort";
    static final String QUERY_TYPE_ATTRIBUTE = "query_type";
    static final String PIT_SCROLL_ATTRIBUTE = "pit_scroll";
    static final String KNN_ATTRIBUTE = "knn";
    static final String TIME_RANGE_FILTER_FIELD_ATTRIBUTE = "time_range_filter_field";
    static final String TIME_RANGE_FILTER_FROM_ATTRIBUTE = "time_range_filter_from";

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

    static String extractIndices(String... indices) {
        try {
            // Note that indices are expected to be resolved, meaning wildcards are not handled on purpose
            // If indices resolve to data streams, the name of the data stream is returned as opposed to its backing indices
            if (indices.length == 1) {
                String index = indices[0];
                assert Regex.isSimpleMatchPattern(index) == false;
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

    public static final String SYSTEM_THREAD_ATTRIBUTE_NAME = "system_thread";
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

    private static void introspectQueryBuilder(QueryBuilder queryBuilder, QueryMetadataBuilder queryMetadataBuilder, int level) {
        if (level > 20) {
            return;
        }
        switch (queryBuilder) {
            case BoolQueryBuilder bool:
                for (QueryBuilder must : bool.must()) {
                    introspectQueryBuilder(must, queryMetadataBuilder, ++level);
                }
                for (QueryBuilder filter : bool.filter()) {
                    introspectQueryBuilder(filter, queryMetadataBuilder, ++level);
                }
                if (bool.must().isEmpty() && bool.filter().isEmpty() && bool.mustNot().isEmpty() && bool.should().size() == 1) {
                    introspectQueryBuilder(bool.should().getFirst(), queryMetadataBuilder, ++level);
                }
                // Note that should clauses are ignored unless there's only one that becomes mandatory
                // must_not clauses are also ignored for now
                break;
            case ConstantScoreQueryBuilder constantScore:
                introspectQueryBuilder(constantScore.innerQuery(), queryMetadataBuilder, ++level);
                break;
            case BoostingQueryBuilder boosting:
                introspectQueryBuilder(boosting.positiveQuery(), queryMetadataBuilder, ++level);
                break;
            case NestedQueryBuilder nested:
                introspectQueryBuilder(nested.query(), queryMetadataBuilder, ++level);
                break;
            case RankDocsQueryBuilder rankDocs:
                QueryBuilder[] queryBuilders = rankDocs.getQueryBuilders();
                if (queryBuilders != null) {
                    for (QueryBuilder builder : queryBuilders) {
                        introspectQueryBuilder(builder, queryMetadataBuilder, level + 1);
                    }
                }
                break;
            case RangeQueryBuilder range:
                // Note that the outcome of this switch differs depending on whether it is executed on the coord node, or data node.
                // Data nodes perform query rewrite on each shard. That means that a query that reports a certain time range filter at the
                // coordinator, may not report the same for all the shards it targets, but rather only for those that do end up executing
                // a true range query at the shard level.
                switch (range.fieldName()) {
                    // don't track unbounded ranges, they translate to either match_none if the field does not exist
                    // or match_all if the field is mapped
                    case TIMESTAMP -> {
                        if (range.to() != null || range.from() != null) {
                            queryMetadataBuilder.rangeOnTimestamp = true;
                        }
                    }
                    case EVENT_INGESTED -> {
                        if (range.to() != null || range.from() != null) {
                            queryMetadataBuilder.rangeOnEventIngested = true;
                        }
                    }
                }
                break;
            case KnnVectorQueryBuilder knn:
                queryMetadataBuilder.knnQuery = true;
                break;
            default:
        }
    }

    private static void introspectRetriever(RetrieverBuilder retrieverBuilder, QueryMetadataBuilder queryMetadataBuilder, int level) {
        if (level > 20) {
            return;
        }
        switch (retrieverBuilder) {
            case KnnRetrieverBuilder knn:
                queryMetadataBuilder.knnQuery = true;
                break;
            case StandardRetrieverBuilder standard:
                introspectQueryBuilder(standard.topDocsQuery(), queryMetadataBuilder, level + 1);
                break;
            case CompoundRetrieverBuilder<?> compound:
                for (CompoundRetrieverBuilder.RetrieverSource retrieverSource : compound.innerRetrievers()) {
                    introspectRetriever(retrieverSource.retriever(), queryMetadataBuilder, level + 1);
                }
                break;
            default:
        }
    }

    private enum TimeRangeBucket {
        FifteenMinutes(TimeValue.timeValueMinutes(15).getMillis(), "15_minutes"),
        OneHour(TimeValue.timeValueHours(1).getMillis(), "1_hour"),
        TwelveHours(TimeValue.timeValueHours(12).getMillis(), "12_hours"),
        OneDay(TimeValue.timeValueDays(1).getMillis(), "1_day"),
        ThreeDays(TimeValue.timeValueDays(3).getMillis(), "3_days"),
        SevenDays(TimeValue.timeValueDays(7).getMillis(), "7_days"),
        FourteenDays(TimeValue.timeValueDays(14).getMillis(), "14_days");

        private final long millis;
        private final String bucketName;

        TimeRangeBucket(long millis, String bucketName) {
            this.millis = millis;
            this.bucketName = bucketName;
        }
    }

    public static void addTimeRangeAttribute(Long timeRangeFrom, long nowInMillis, Map<String, Object> attributes) {
        if (timeRangeFrom != null) {
            String timestampRangeFilter = introspectTimeRange(timeRangeFrom, nowInMillis);
            attributes.put(TIME_RANGE_FILTER_FROM_ATTRIBUTE, timestampRangeFilter);
        }
    }

    static String introspectTimeRange(long timeRangeFromMillis, long nowInMillis) {
        for (TimeRangeBucket value : TimeRangeBucket.values()) {
            if (timeRangeFromMillis >= nowInMillis - value.millis) {
                return value.bucketName;
            }
        }
        return "older_than_14_days";
    }
}
