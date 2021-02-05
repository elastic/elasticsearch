/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rollup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RollupGroup;
import org.elasticsearch.cluster.metadata.RollupMetadata;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.query.SearchExecutionContext;
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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class RollupShardDecider {
    private static final Logger logger = LogManager.getLogger(RollupShardDecider.class);

    public static final Set<String> SUPPORTED_AGGS = Set.of(
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
                                   SearchExecutionContext context,
                                   IndexMetadata requestIndexMetadata, RollupMetadata rollupMetadata,
                                   String[] indices,
                                   SortedMap<String, IndexAbstraction> indexLookup) {
        IndexAbstraction originalIndex = indexLookup.get(requestIndexMetadata.getIndex().getName());
        // Index must be member of a datastream
        if (originalIndex.getParentDataStream() == null) {
            return true;
        }

        // Rollup metadata must exist
        if (rollupMetadata == null) {
            return true;
        }

        final String requestIndexName = requestIndexMetadata.getIndex().getName();
        final AggregatorFactories.Builder aggregations = request.source() != null ? request.source().aggregations() : null;

        if (isRollupIndex(requestIndexMetadata)) { // A rollup index is being searched
            if (checkRollupConditions(request) == false) {
                return false;
            }

            Map<String, String> indexRollupMetadata = requestIndexMetadata.getCustomData(RollupMetadata.TYPE);
            final String originalIndexName = indexRollupMetadata.get(RollupMetadata.SOURCE_INDEX_NAME_META_FIELD);
            final RollupGroup rollupGroup = rollupMetadata.rollupGroups().get(originalIndexName);

            String optimalIndex = findOptimalIndex(originalIndexName, rollupGroup, aggregations);
            logger.info("Requested index: " + requestIndexName + " - Optimal index: " + optimalIndex);

            return requestIndexName.equals(optimalIndex);
        } else if (rollupMetadata.contains(requestIndexName) && checkRollupConditions(request)) { // There are rollup indices for this index
            final RollupGroup rollupGroup = rollupMetadata.rollupGroups().get(requestIndexName);
            String optimalIndex = findOptimalIndex(requestIndexName, rollupGroup, aggregations);
            logger.info("Requested index: " + requestIndexName + " - Optimal index: " + optimalIndex);
            return requestIndexName.equals(optimalIndex);
        } else {
            // Not part of a rollup group or rollups cannot serve the query, search away!
            return true;
        }
    }

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

        //TODO(csoulios): Add check for runtime fields in the request
        return true;
    }

    /**
     * Check if requested aggregations are supported by rollup indices
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

    public static boolean isRollupIndex(IndexMetadata requestIndexMetadata) {
        return requestIndexMetadata.getCustomData(RollupMetadata.TYPE) != null;
    }

    static String findOptimalIndex(String originalIndexName, RollupGroup rollupGroup, AggregatorFactories.Builder aggFactoryBuilders) {
        DateHistogramAggregationBuilder dateHistogramBuilder = getDateHistogramAggregationBuilder(aggFactoryBuilders);
        return findOptimalIntervalIndex(originalIndexName, rollupGroup, dateHistogramBuilder);
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
     * @param originalIndex The original index with raw data
     * @param rollupGroup The group of rollup indices that are candidates
     * @param source The source of the aggregation in the request
     * @return the name of the optimal (maximum interval) index that matches the query
     */
    static String findOptimalIntervalIndex(String originalIndex, RollupGroup rollupGroup, DateHistogramAggregationBuilder source) {
        String optimalIndex = originalIndex;
        ZoneId sourceTimeZone = ZoneOffset.UTC;
        if (source != null && source.timeZone() != null) {
            sourceTimeZone = ZoneId.of(source.timeZone().toString(), ZoneId.SHORT_IDS);
        }
        DateHistogramInterval sourceInterval = source != null ? source.getCalendarInterval() : null;

        DateHistogramInterval maxInterval = null;
        for (String rollupIndex : rollupGroup.getIndices()) {
            ZoneId thisTimezone = rollupGroup.getDateTimezone(rollupIndex).zoneId();
            if (sourceTimeZone.getRules().equals(thisTimezone.getRules()) == false) {
                // Incompatible timezone => skip this rollup group
                continue;
            }

            DateHistogramInterval thisInterval = rollupGroup.getDateInterval(rollupIndex);
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
     * @param requiredInterval the required interval to match
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
}
