/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rollup;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RollupIndexMetadata;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class RollupShardDecider {

    static final Set<String> SUPPORTED_AGGS = Set.of(
        DateHistogramAggregationBuilder.NAME,
        TermsAggregationBuilder.NAME,
        MinAggregationBuilder.NAME,
        MaxAggregationBuilder.NAME,
        SumAggregationBuilder.NAME,
        ValueCountAggregationBuilder.NAME,
        AvgAggregationBuilder.NAME
    );

    /**
     * Decide if an index can match a query. When indices are rollup indices, all
     * requirements for matching the query/aggregation will be considered.
     *
     * @return The response of the can_match phase. The response is enriched with information
     * about the source index that a rollup index came from, as well as the priority assigned
     * to rollup indices.
     */
    public static SearchService.CanMatchResponse canMatch(ShardSearchRequest request,
                                                          SearchExecutionContext context,
                                                          IndexMetadata requestIndexMetadata,
                                                          SortedMap<String, IndexAbstraction> indexLookup) throws IOException {
        IndexAbstraction originalIndex = indexLookup.get(requestIndexMetadata.getIndex().getName());
        String sourceIndex = requestIndexMetadata.getIndex().getName(); // TODO: change to requestIndexMetadata.getIndex().getUUID()
        Map<String, String> indexRollupMetadata = requestIndexMetadata.getCustomData(RollupIndexMetadata.TYPE);

        // Index must be member of a data stream and rollup metadata must exist in the index metadata
        if (originalIndex.getParentDataStream() == null || indexRollupMetadata == null) {
            return new SearchService.CanMatchResponse(true, null, sourceIndex, null);
        }

        // A rollup index is being searched
        if (validateRollupConditions(request, context, requestIndexMetadata) == false) {
            return new SearchService.CanMatchResponse(false, null, sourceIndex, null);
        }

        final AggregatorFactories.Builder aggregations = request.source() != null ? request.source().aggregations() : null;
        final String rollupConfigSource = indexRollupMetadata.get(RollupIndexMetadata.ROLLUP_META_FIELD);
        sourceIndex = indexRollupMetadata.get(RollupIndexMetadata.SOURCE_INDEX_NAME_META_FIELD);
        RollupIndexMetadata rollupIndexMetadata = RollupIndexMetadata.parseMetadataXContent(rollupConfigSource);

        DateHistogramAggregationBuilder source = getDateHistogramAggregationBuilder(aggregations);
        ZoneId sourceTimeZone = ZoneOffset.UTC; // Default timezone is UTC
        if (source != null && source.timeZone() != null) {
            sourceTimeZone = ZoneId.of(source.timeZone().toString(), ZoneId.SHORT_IDS);
        }
        DateHistogramInterval sourceInterval = source != null ? source.getCalendarInterval() : null;
        // Incompatible timezone => skip this rollup index
        if (canMatchTimezone(sourceTimeZone, rollupIndexMetadata.getDateTimezone().zoneId()) == false
            || canMatchCalendarInterval(sourceInterval, rollupIndexMetadata.getDateInterval()) == false) {
            return new SearchService.CanMatchResponse(false, null);
        }
        // Assign index priority to match the rollup interval. Higher intervals have higher priority
        // Index priority be evaluated at the coordinator node to select the optimal shard
        long priority = rollupIndexMetadata.getDateInterval().estimateMillis();

        return new SearchService.CanMatchResponse(true, null, sourceIndex, priority);
    }

    /**
     * Parses the search request and checks if the required conditions are met so that results
     * can be answered by a rollup index.
     *
     * Conditions that must be met are:
     *  - Request size must be 0
     *  - Metric aggregations must be supported
     *  - No use of runtime fields in the query
     *
     * @param request the search request to parse
     * @return true if a rollup index can
     */
    private static boolean validateRollupConditions(ShardSearchRequest request, SearchExecutionContext context,
                                                    IndexMetadata requestIndexMetadata) {
        if (request.source() == null) {
            return false;
        }

        // If request size is not 0, rollup indices should not match
        if (request.source().size() > 0) {
            return false;
        }

        // Check for supported aggs
        AggregatorFactories.Builder aggregations = request.source().aggregations();
        if (checkSupportedAggregations(aggregations) == false){
            return false;
        }

        // If runtime fields are used in the query, rollup indices should not match
        if (request.getRuntimeMappings().isEmpty() == false) {
            return false;
        }
        return true;
    }

    /**
     * Check if requested aggregations are supported by rollup indices
     *
     * @param aggregations the aggregation builders
     * @return true if aggregations are supported by rollups, otherwise false
     */
    private static boolean checkSupportedAggregations(AggregatorFactories.Builder aggregations) {
        if (aggregations == null) {
            return false;
        }

        for (AggregationBuilder builder : aggregations.getAggregatorFactories()) {
            if (SUPPORTED_AGGS.contains(builder.getWriteableName()) == false) {
                return false;
            }
        }

        return true;
    }

    /**
     * Parse the aggregator factories and return a date_histogram {@link AggregationBuilder} for the aggregation on rollups
     */
    private static DateHistogramAggregationBuilder getDateHistogramAggregationBuilder(AggregatorFactories.Builder aggFactoryBuilders) {
        DateHistogramAggregationBuilder dateHistogramBuilder = null;
        for (AggregationBuilder builder : aggFactoryBuilders.getAggregatorFactories()) {
            if (builder.getWriteableName().equals(DateHistogramAggregationBuilder.NAME)) {
                dateHistogramBuilder =  (DateHistogramAggregationBuilder) builder;
            }
        }
        return dateHistogramBuilder;
    }

    /**
     * Validate if a candidate interval can match the required accuracy for a given interval. A candidate interval
     * matches the required interval only if it has greater or equal accuracy to the required interval. This means
     * that the base unit (1h, 1d, 1M etc) of the candidate interval must be smaller or equal to the base unit
     * of the required interval.
     *
     * @param requiredInterval  the required interval to match. If null, all candidateIntervals will match.
     * @param candidateInterval the candidate inteval to validate
     * @return true if the candidate interval can match the required interval, otherwise false
     */
    static boolean canMatchCalendarInterval(DateHistogramInterval requiredInterval, DateHistogramInterval candidateInterval) {
        // If no interval is required, any interval should do
        if (requiredInterval == null) {
            return true;
        }

        // If candidate interval is empty,
        if (candidateInterval == null) {
            return false;
        }

        // The request must be gte the config. The CALENDAR_ORDERING map values are integers representing
        // relative orders between the calendar units
        Rounding.DateTimeUnit requiredIntervalUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(requiredInterval.toString());
        if (requiredIntervalUnit == null) {
            return false;
        }
        Rounding.DateTimeUnit candidateIntervalUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(candidateInterval.toString());
        if (candidateIntervalUnit == null) {
            return false;
        }

        long requiredIntervalOrder = requiredIntervalUnit.getField().getBaseUnit().getDuration().toMillis();
        long candidateIntervalOrder = candidateIntervalUnit.getField().getBaseUnit().getDuration().toMillis();

        // All calendar units are multiples naturally, so we just care about gte
        return requiredIntervalOrder >= candidateIntervalOrder;
    }

    /**
     * Check if two timezones are compatible.
     *
     * @return true if the timezones are compatible, otherwise false
     */
    static boolean canMatchTimezone(ZoneId tz1, ZoneId tz2) {
        if (tz1 == null || tz2 == null) {
            throw new IllegalArgumentException("Timezone cannot be null");
        }
        return tz1.getRules().equals(tz2.getRules());
    }
}
