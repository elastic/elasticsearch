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
     * Decide if index can be matched considering rollup indices
     */
    public static boolean canMatch(ShardSearchRequest request,
                                   IndexMetadata requestIndexMetadata,
                                   SortedMap<String, IndexAbstraction> indexLookup) throws IOException {
        IndexAbstraction originalIndex = indexLookup.get(requestIndexMetadata.getIndex().getName());
        // Index must be member of a data stream
        if (originalIndex.getParentDataStream() == null) {
            return true;
        }
        // Rollup metadata must exist
        Map<String, String> indexRollupMetadata = requestIndexMetadata.getCustomData(RollupIndexMetadata.TYPE);
        if (indexRollupMetadata == null) {
            return true;
        }
        // A rollup index is being searched
        if (checkRollupConditions(request) == false) {
            return false;
        }

        final AggregatorFactories.Builder aggregations = request.source() != null ? request.source().aggregations() : null;
        final String rollupConfigSource = indexRollupMetadata.get(RollupIndexMetadata.ROLLUP_META_FIELD);
        RollupIndexMetadata rollupIndexMetadata = RollupIndexMetadata.parseMetadataXContent(rollupConfigSource);

        DateHistogramAggregationBuilder source = getDateHistogramAggregationBuilder(aggregations);
        ZoneId sourceTimeZone = ZoneOffset.UTC; // Default timezone is UTC
        if (source != null && source.timeZone() != null) {
            sourceTimeZone = ZoneId.of(source.timeZone().toString(), ZoneId.SHORT_IDS);
        }
        DateHistogramInterval sourceInterval = source != null ? source.getCalendarInterval() : null;
        // Incompatible timezone => skip this rollup group
        if (canMatchTimezone(sourceTimeZone, rollupIndexMetadata.getDateTimezone().zoneId()) == false
            || canMatchCalendarInterval(sourceInterval, rollupIndexMetadata.getDateInterval()) == false) {
            return false;
        }

        return true;
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
    private static boolean checkRollupConditions(ShardSearchRequest request) {
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

        // TODO(csoulios): Add check for runtime fields in the request
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
     * From a collection of rollup group indices, find the optimal rollup index to match a list of aggregations.
     *
     * @param rollupGroup  a map containing the names and {@link RollupIndexMetadata} for indices to be compared
     * @param aggregations the aggregations that the rollup indices will be queried for
     * @return the name of the optimal rollup index
     */
    public static String findOptimalIndex(Map<String, RollupIndexMetadata> rollupGroup, AggregatorFactories.Builder aggregations) {
        DateHistogramAggregationBuilder dateHistogramBuilder = getDateHistogramAggregationBuilder(aggregations);
        return findOptimalIntervalIndex(rollupGroup, dateHistogramBuilder);
    }

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
     * Find the index that best matches the date histogram interval requested in the date_histogram
     * aggregation source. If there are more than one rollup indices, we always try to find the largest
     * interval that matches the aggregation. If no rollup index matches the interval, we return the
     * original index.
     *
     * @param rollupGroup The group of rollup indices that are candidates
     * @param source The source of the aggregation in the request
     * @return the name of the optimal (maximum interval) index that matches the query
     */
    static String findOptimalIntervalIndex(Map<String, RollupIndexMetadata> rollupGroup, DateHistogramAggregationBuilder source) {
        DateHistogramInterval sourceInterval = source != null ? source.getCalendarInterval() : null;
        String optimalIndex = null;
        DateHistogramInterval maxInterval = null;
        for (Map.Entry<String, RollupIndexMetadata> entry : rollupGroup.entrySet()) {
            String rollupIndex = entry.getKey();
            RollupIndexMetadata metadata = entry.getValue();

            DateHistogramInterval thisInterval = metadata.getDateInterval();
            if (canMatchCalendarInterval(sourceInterval, thisInterval)) {
                if (maxInterval == null || canMatchCalendarInterval(thisInterval, maxInterval)) {
                    optimalIndex = rollupIndex;
                    maxInterval = thisInterval;
                }
            }
        }
        return optimalIndex;
    }

    /**
     * Validate if a candidate interval can match the required accuracy for a given interval. A candidate interval
     * matches the required interval only if it has greater or equal accuracy to the required interval. This means
     * that the base unit (1h, 1d, 1M etc) of the candidate interval must be smaller or equal to the base unit
     * of the required interval.
     *
     * @param requiredInterval  the required interval to match
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
    private static boolean canMatchTimezone(ZoneId tz1, ZoneId tz2) {
        return tz1.getRules().equals(tz2.getRules());
    }
}
