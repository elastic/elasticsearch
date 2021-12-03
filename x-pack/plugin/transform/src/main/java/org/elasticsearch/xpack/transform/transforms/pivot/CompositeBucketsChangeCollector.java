/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.GeoTileGridValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Utility class to collect bucket changes
 */
public class CompositeBucketsChangeCollector implements ChangeCollector {

    // a magic for the case that we do not need a composite aggregation to collect changes
    private static final Map<String, Object> AFTER_KEY_MAGIC_FOR_NON_COMPOSITE_COLLECTORS = Collections.singletonMap(
        "_transform",
        "no_composite_after_key"
    );

    private static final String COMPOSITE_AGGREGATION_NAME = "_transform_change_collector";
    private final Map<String, FieldCollector> fieldCollectors;
    private final CompositeAggregationBuilder compositeAggregation;

    /**
     * Collector for collecting changes from 1 group_by field.
     *
     * Every field collector instance is stateful and implements the query logic and result collection,
     * but also stores the changes in their state.
     */
    interface FieldCollector {

        /**
         * Get the maximum page size supported by this field collector.
         *
         * Note: this page size is only about change collection, not the indexer page size.
         *
         * @return the maximum allowed page size, or Integer.MAX_VALUE for unlimited.
         */
        int getMaxPageSize();

        /**
         * Allows the field collector to add aggregations to the changes query.
         *
         * @return a collection of aggregations
         */
        Collection<AggregationBuilder> aggregateChanges();

        /**
         * Collect the results from the added aggregations in aggregateChanges.
         *
         * @return true if this collection is done and there are no more changes to look for
         */
        boolean collectChangesFromAggregations(Aggregations aggregations);

        /**
         * Return a composite value source builder if the collector requires it.
         *
         * @return {@link CompositeValuesSourceBuilder} instance or null.
         */
        CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder();

        /**
         * Collects the changes from the search response, e.g. stores the terms that have changed.
         *
         * If the field collector implements this method, requiresCompositeSources must return true,
         * otherwise requiresCompositeSources should return false.
         *
         * @param buckets buckets from the search result.
         * @return true if this collection is done and there are no more changes to look for
         */
        boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets);

        /**
         * Apply the collected changes in the query that updates the transform destination.
         *
         * @param lastCheckpointTimestamp the last(complete) checkpoint timestamp
         * @param nextCheckpointTimestamp the next(currently running) checkpoint timestamp.
         * @return a querybuilder instance with added filters to narrow the search
         */
        QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextCheckpointTimestamp);

        /**
         * Clear the field collector, e.g. the changes to free up memory.
         */
        void clear();

        /**
         * Whether the collector optimizes change detection by narrowing the required query.
         *
         * @return true if the collector optimizes change detection
         */
        boolean isOptimized();

        /**
         * Whether the collector requires an extra query to identify the changes.
         *
         * @return true if collector requires an extra query for identifying changes
         */
        boolean queryForChanges();
    }

    static class TermsFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;
        private final boolean missingBucket;
        private final Set<String> changedTerms;
        // although we could add null to the hash set, its easier to handle null separately
        private boolean foundNullBucket;

        TermsFieldCollector(final String sourceFieldName, final String targetFieldName, final boolean missingBucket) {
            assert sourceFieldName != null;
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
            this.missingBucket = missingBucket;
            this.changedTerms = new HashSet<>();
            this.foundNullBucket = false;
        }

        @Override
        public int getMaxPageSize() {
            // TODO: based on index.max_terms_count, however this is per index, which we don't have access to here,
            // because the page size is limit to 64k anyhow, return 64k
            return 65536;
        }

        @Override
        public CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder() {
            return new TermsValuesSourceBuilder(targetFieldName).field(sourceFieldName).missingBucket(missingBucket);
        }

        @Override
        public boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets) {
            changedTerms.clear();
            foundNullBucket = false;

            for (Bucket b : buckets) {
                Object term = b.getKey().get(targetFieldName);
                if (term != null) {
                    changedTerms.add(term.toString());
                } else {
                    // we should not find a null bucket if missing bucket is false
                    assert missingBucket;
                    foundNullBucket = true;
                }
            }

            // if buckets have been found, we need another run
            return buckets.isEmpty();
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            if (missingBucket && foundNullBucket) {
                QueryBuilder missingBucketQuery = new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(sourceFieldName));

                if (changedTerms.isEmpty()) {
                    return missingBucketQuery;
                }

                /**
                 * Combined query with terms and missing bucket:
                 *
                 * "bool": {
                 *   "should": [
                 *     {
                 *       "terms": {
                 *         "source_field": [
                 *           "term1",
                 *           "term2",
                 *           ...
                 *         ]
                 *       }
                 *     },
                 *     {
                 *       "bool": {
                 *         "must_not": [
                 *           {
                 *             "exists": {
                 *               "field": "source_field"
                 *             }
                 *           }
                 *         ]
                 *       }
                 *     }
                 *   ]
                 * }
                 */
                return new BoolQueryBuilder().should(new TermsQueryBuilder(sourceFieldName, changedTerms)).should(missingBucketQuery);

            } else if (changedTerms.isEmpty() == false) {
                return new TermsQueryBuilder(sourceFieldName, changedTerms);
            }
            return null;
        }

        @Override
        public void clear() {
            changedTerms.clear();
            foundNullBucket = false;
        }

        @Override
        public Collection<AggregationBuilder> aggregateChanges() {
            return Collections.emptyList();
        }

        @Override
        public boolean collectChangesFromAggregations(Aggregations aggregations) {
            return true;
        }

        @Override
        public boolean isOptimized() {
            return true;
        }

        @Override
        public boolean queryForChanges() {
            return true;
        }
    }

    /**
     * Date histogram field collector for the case that it shares the same timestamp field as for sync
     *
     * Note: does not support missing_bucket
     */
    static class DateHistogramFieldCollectorSynchronized implements FieldCollector {

        private final String sourceFieldName;
        private final Rounding.Prepared rounding;

        DateHistogramFieldCollectorSynchronized(
            final String sourceFieldName,
            final String targetFieldName,
            final boolean missingBucket,
            final Rounding.Prepared rounding
        ) {
            assert sourceFieldName != null;
            assert missingBucket == false;
            this.sourceFieldName = sourceFieldName;
            this.rounding = rounding;
        }

        @Override
        public int getMaxPageSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder() {
            return null;
        }

        @Override
        public boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets) {
            return true;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            return new RangeQueryBuilder(sourceFieldName).gte(rounding.round(lastCheckpointTimestamp)).format("epoch_millis");
        }

        @Override
        public void clear() {}

        @Override
        public Collection<AggregationBuilder> aggregateChanges() {
            return Collections.emptyList();
        }

        @Override
        public boolean collectChangesFromAggregations(Aggregations aggregations) {
            return true;
        }

        @Override
        public boolean isOptimized() {
            return true;
        }

        @Override
        public boolean queryForChanges() {
            // for this collector we do not need an extra run, because we can narrow the query given the checkpoints
            return false;
        }
    }

    /**
     * Generic date histogram field collector
     */
    static class DateHistogramFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final boolean missingBucket;
        private final Rounding.Prepared rounding;
        private final Collection<AggregationBuilder> timeFieldAggregations;
        private final String minAggregationOutputName;
        private final String maxAggregationOutputName;

        private long lowerBound;
        private long upperBound;

        DateHistogramFieldCollector(
            final String sourceFieldName,
            final String targetFieldName,
            final boolean missingBucket,
            final Rounding.Prepared rounding
        ) {
            assert sourceFieldName != null;
            this.sourceFieldName = sourceFieldName;
            this.missingBucket = missingBucket;
            this.rounding = rounding;
            minAggregationOutputName = COMPOSITE_AGGREGATION_NAME + "." + targetFieldName + ".min";
            maxAggregationOutputName = COMPOSITE_AGGREGATION_NAME + "." + targetFieldName + ".max";

            // the time field for the date histogram is different than for sync
            timeFieldAggregations = new ArrayList<>();
            timeFieldAggregations.add(AggregationBuilders.min(minAggregationOutputName).field(sourceFieldName));
            timeFieldAggregations.add(AggregationBuilders.max(maxAggregationOutputName).field(sourceFieldName));
        }

        @Override
        public int getMaxPageSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder() {
            return null;
        }

        @Override
        public boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets) {
            return true;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            if (missingBucket) {
                return null;
            }
            return new RangeQueryBuilder(sourceFieldName).gte(lowerBound).lte(upperBound).format("epoch_millis");
        }

        @Override
        public void clear() {}

        @Override
        public Collection<AggregationBuilder> aggregateChanges() {
            // optimization can't be applied for missing bucket
            return missingBucket ? Collections.emptyList() : timeFieldAggregations;
        }

        @Override
        public boolean collectChangesFromAggregations(Aggregations aggregations) {
            final SingleValue lowerBoundResult = aggregations.get(minAggregationOutputName);
            final SingleValue upperBoundResult = aggregations.get(maxAggregationOutputName);

            if (lowerBoundResult != null && upperBoundResult != null) {
                // we only need to round the lower bound, because the checkpoint will not contain new data for the upper bound
                lowerBound = rounding.round((long) lowerBoundResult.value());
                upperBound = (long) upperBoundResult.value();

                return false;
            }

            return true;
        }

        @Override
        public boolean isOptimized() {
            // not optimized if missing bucket is true
            return missingBucket == false;
        }

        @Override
        public boolean queryForChanges() {
            // not optimized if missing bucket is true
            return missingBucket == false;
        }
    }

    static class HistogramFieldCollector implements FieldCollector {

        // cutoff is calculated with max_range/current_range, current_range must be smaller
        // the optimization gets only applied if we cut at least by 20%
        private static final double MIN_CUT_OFF = 1.2;
        private final String sourceFieldName;
        private final boolean missingBucket;
        private final double interval;
        private final Collection<AggregationBuilder> histogramFieldAggregations;
        private final String minAggregationOutputName;
        private final String maxAggregationOutputName;

        private double minLowerBound;
        private double maxUpperBound;

        private double lowerBound;
        private double upperBound;

        HistogramFieldCollector(
            final String sourceFieldName,
            final String targetFieldName,
            final boolean missingBucket,
            final double interval
        ) {
            assert sourceFieldName != null;
            this.sourceFieldName = sourceFieldName;
            this.missingBucket = missingBucket;

            this.interval = interval;

            minAggregationOutputName = COMPOSITE_AGGREGATION_NAME + "." + targetFieldName + ".min";
            maxAggregationOutputName = COMPOSITE_AGGREGATION_NAME + "." + targetFieldName + ".max";

            histogramFieldAggregations = new ArrayList<>();
            histogramFieldAggregations.add(AggregationBuilders.min(minAggregationOutputName).field(sourceFieldName));
            histogramFieldAggregations.add(AggregationBuilders.max(maxAggregationOutputName).field(sourceFieldName));
        }

        @Override
        public int getMaxPageSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder() {
            return null;
        }

        @Override
        public boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets) {
            return true;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            if (missingBucket) {
                return null;
            }

            // (upperBound - lowerBound) >= interval, so never 0
            if ((maxUpperBound - minLowerBound) / (upperBound - lowerBound) < MIN_CUT_OFF) {
                return null;
            }

            return new RangeQueryBuilder(sourceFieldName).gte(lowerBound).lt(upperBound);
        }

        @Override
        public void clear() {}

        @Override
        public Collection<AggregationBuilder> aggregateChanges() {
            // optimization can't be applied for missing bucket
            return missingBucket ? Collections.emptyList() : histogramFieldAggregations;
        }

        @Override
        public boolean collectChangesFromAggregations(Aggregations aggregations) {
            final SingleValue lowerBoundResult = aggregations.get(minAggregationOutputName);
            final SingleValue upperBoundResult = aggregations.get(maxAggregationOutputName);

            if (lowerBoundResult != null && upperBoundResult != null) {
                lowerBound = interval * (Math.floor(lowerBoundResult.value() / interval));
                upperBound = interval * (1 + Math.floor(upperBoundResult.value() / interval));

                minLowerBound = Math.min(minLowerBound, lowerBound);
                maxUpperBound = Math.max(maxUpperBound, upperBound);
                return false;
            }

            return true;
        }

        @Override
        public boolean isOptimized() {
            // not optimized if missing bucket is true
            return missingBucket == false;
        }

        @Override
        public boolean queryForChanges() {
            // not optimized if missing bucket is true
            return missingBucket == false;
        }
    }

    static class GeoTileFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;
        private final boolean missingBucket;
        private final Set<String> changedBuckets;
        // although we could add null to the hash set, its easier to handle null separately
        private boolean foundNullBucket;

        GeoTileFieldCollector(final String sourceFieldName, final String targetFieldName, final boolean missingBucket) {
            assert sourceFieldName != null;
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
            this.missingBucket = missingBucket;
            this.changedBuckets = new HashSet<>();
            this.foundNullBucket = false;
        }

        @Override
        public int getMaxPageSize() {
            // this collector is limited by indices.query.bool.max_clause_count, default 1024
            return BooleanQuery.getMaxClauseCount();
        }

        @Override
        public CompositeValuesSourceBuilder<?> getCompositeValueSourceBuilder() {
            return new GeoTileGridValuesSourceBuilder(targetFieldName).field(sourceFieldName).missingBucket(missingBucket);
        }

        @Override
        public boolean collectChangesFromCompositeBuckets(Collection<? extends Bucket> buckets) {
            changedBuckets.clear();
            foundNullBucket = false;

            for (Bucket b : buckets) {
                Object bucket = b.getKey().get(targetFieldName);
                if (bucket != null) {
                    changedBuckets.add(bucket.toString());
                } else {
                    // we should not find a null bucket if missing bucket is false
                    assert missingBucket;
                    foundNullBucket = true;
                }
            }

            return buckets.isEmpty();
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            BoolQueryBuilder boundingBoxesQueryBuilder = null;

            if (changedBuckets.isEmpty() == false) {
                boundingBoxesQueryBuilder = QueryBuilders.boolQuery();
                changedBuckets.stream().map(GeoTileUtils::toBoundingBox).map(this::toGeoQuery).forEach(boundingBoxesQueryBuilder::should);
            }

            if (missingBucket && foundNullBucket) {
                QueryBuilder missingBucketQuery = new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(sourceFieldName));

                if (boundingBoxesQueryBuilder == null) {
                    return missingBucketQuery;
                }

                /**
                 * Combined query with geo bounding boxes and missing bucket:
                 *
                 * "bool": {
                 *   "should": [
                 *     {
                 *       "geo_bounding_box": {
                 *         "source_field": {
                 *           "top_left": {
                 *             "lat": x1,
                 *             "lon": y1
                 *           },
                 *           "bottom_right": {
                 *             "lat": x2,
                 *             "lon": y2
                 *           }
                 *         }
                 *       }
                 *     },
                 *     {
                 *       "geo_bounding_box": {
                 *         ...
                 *       }
                 *     },
                 *     {
                 *       "bool": {
                 *         "must_not": [
                 *           {
                 *             "exists": {
                 *               "field": "source_field"
                 *             }
                 *           }
                 *         ]
                 *       }
                 *     }
                 *   ]
                 * }
                 */
                return boundingBoxesQueryBuilder.should(missingBucketQuery);
            }

            return boundingBoxesQueryBuilder;
        }

        @Override
        public void clear() {
            changedBuckets.clear();
            foundNullBucket = false;
        }

        @Override
        public Collection<AggregationBuilder> aggregateChanges() {
            return Collections.emptyList();
        }

        @Override
        public boolean collectChangesFromAggregations(Aggregations aggregations) {
            return true;
        }

        private GeoBoundingBoxQueryBuilder toGeoQuery(Rectangle rectangle) {
            return QueryBuilders.geoBoundingBoxQuery(sourceFieldName)
                .setCorners(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                );
        }

        @Override
        public boolean isOptimized() {
            return true;
        }

        @Override
        public boolean queryForChanges() {
            return false;
        }
    }

    private CompositeBucketsChangeCollector(
        @Nullable CompositeAggregationBuilder compositeAggregation,
        Map<String, FieldCollector> fieldCollectors
    ) {
        this.compositeAggregation = compositeAggregation;
        this.fieldCollectors = fieldCollectors;
    }

    @Override
    public SearchSourceBuilder buildChangesQuery(SearchSourceBuilder sourceBuilder, Map<String, Object> position, int pageSize) {
        sourceBuilder.size(0);
        for (FieldCollector fieldCollector : fieldCollectors.values()) {

            // add aggregations, but only for the 1st run
            if (position == null || position.isEmpty()) {
                for (AggregationBuilder fieldCollectorAgg : fieldCollector.aggregateChanges()) {
                    sourceBuilder.aggregation(fieldCollectorAgg);
                }
            }
            pageSize = Math.min(pageSize, fieldCollector.getMaxPageSize());
        }

        if (compositeAggregation != null) {
            CompositeAggregationBuilder changesAgg = compositeAggregation;
            changesAgg.size(pageSize).aggregateAfter(position);
            sourceBuilder.aggregation(changesAgg);
        }

        return sourceBuilder;
    }

    @Override
    public QueryBuilder buildFilterQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        // shortcut for only 1 element
        if (fieldCollectors.size() == 1) {
            return fieldCollectors.values()
                .iterator()
                .next()
                .filterByChanges(lastCheckpoint.getTimeUpperBound(), nextCheckpoint.getTimeUpperBound());
        }

        BoolQueryBuilder filteredQuery = new BoolQueryBuilder();

        for (FieldCollector fieldCollector : fieldCollectors.values()) {
            QueryBuilder filter = fieldCollector.filterByChanges(lastCheckpoint.getTimeUpperBound(), nextCheckpoint.getTimeUpperBound());
            if (filter != null) {
                filteredQuery.filter(filter);
            }
        }

        return filteredQuery;
    }

    @Override
    public Collection<String> getIndicesToQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        // for updating the data, all indices have to be queried
        return TransformCheckpoint.getChangedIndices(TransformCheckpoint.EMPTY, nextCheckpoint);
    }

    @Override
    public Map<String, Object> processSearchResponse(final SearchResponse searchResponse) {
        final Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return null;
        }

        // every collector reports if the collection of changes is done after processing, if all are done, the indexer
        // will not run a query for change collection again
        boolean isDone = true;

        // collect changes from aggregations added by field collectors
        for (FieldCollector fieldCollector : fieldCollectors.values()) {
            isDone &= fieldCollector.collectChangesFromAggregations(aggregations);
        }

        // collect changes from composite buckets
        final CompositeAggregation compositeAggResults = aggregations.get(COMPOSITE_AGGREGATION_NAME);

        // xor: either both must exist or not exist
        assert (compositeAggregation == null ^ compositeAggResults == null) == false;

        if (compositeAggResults != null) {
            Collection<? extends Bucket> buckets = compositeAggResults.getBuckets();
            for (FieldCollector fieldCollector : fieldCollectors.values()) {
                isDone &= fieldCollector.collectChangesFromCompositeBuckets(buckets);
            }

            if (isDone == false) {
                return compositeAggResults.afterKey();
            }
        } else if (isDone == false) {
            // if we don't have a composite aggregation, we need a magic key, because the indexer handles the state
            return AFTER_KEY_MAGIC_FOR_NON_COMPOSITE_COLLECTORS;
        }

        return null;
    }

    @Override
    public void clear() {
        fieldCollectors.forEach((k, c) -> c.clear());
    }

    @Override
    public boolean isOptimized() {
        return fieldCollectors.values().stream().anyMatch(FieldCollector::isOptimized);
    }

    @Override
    public boolean queryForChanges() {
        return fieldCollectors.values().stream().anyMatch(FieldCollector::queryForChanges);
    }

    public static ChangeCollector buildChangeCollector(Map<String, SingleGroupSource> groups, String synchronizationField) {
        Map<String, FieldCollector> fieldCollectors = createFieldCollectors(groups, synchronizationField);
        return new CompositeBucketsChangeCollector(createCompositeAgg(fieldCollectors), fieldCollectors);
    }

    private static CompositeAggregationBuilder createCompositeAgg(Map<String, FieldCollector> fieldCollectors) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();

        for (FieldCollector fc : fieldCollectors.values()) {
            CompositeValuesSourceBuilder<?> sourceBuilder = fc.getCompositeValueSourceBuilder();
            if (sourceBuilder != null) {
                sources.add(sourceBuilder);
            }
        }

        if (sources.isEmpty()) {
            return null;
        }

        return new CompositeAggregationBuilder(COMPOSITE_AGGREGATION_NAME, sources);
    }

    static Map<String, FieldCollector> createFieldCollectors(Map<String, SingleGroupSource> groups, String synchronizationField) {
        Map<String, FieldCollector> fieldCollectors = new HashMap<>();

        for (Entry<String, SingleGroupSource> entry : groups.entrySet()) {
            // skip any fields that use scripts
            if (entry.getValue().getScriptConfig() != null) {
                continue;
            }

            switch (entry.getValue().getType()) {
                case TERMS:
                    fieldCollectors.put(
                        entry.getKey(),
                        new CompositeBucketsChangeCollector.TermsFieldCollector(
                            entry.getValue().getField(),
                            entry.getKey(),
                            entry.getValue().getMissingBucket()
                        )
                    );
                    break;
                case HISTOGRAM:
                    fieldCollectors.put(
                        entry.getKey(),
                        new CompositeBucketsChangeCollector.HistogramFieldCollector(
                            entry.getValue().getField(),
                            entry.getKey(),
                            entry.getValue().getMissingBucket(),
                            ((HistogramGroupSource) entry.getValue()).getInterval()
                        )
                    );
                    break;
                case DATE_HISTOGRAM:
                    fieldCollectors.put(
                        entry.getKey(),
                        entry.getValue().getMissingBucket() == false
                            && entry.getValue().getField() != null
                            && entry.getValue().getField().equals(synchronizationField)
                                ? new CompositeBucketsChangeCollector.DateHistogramFieldCollectorSynchronized(
                                    entry.getValue().getField(),
                                    entry.getKey(),
                                    entry.getValue().getMissingBucket(),
                                    ((DateHistogramGroupSource) entry.getValue()).getRounding()
                                )
                                : new CompositeBucketsChangeCollector.DateHistogramFieldCollector(
                                    entry.getValue().getField(),
                                    entry.getKey(),
                                    entry.getValue().getMissingBucket(),
                                    ((DateHistogramGroupSource) entry.getValue()).getRounding()
                                )
                    );
                    break;
                case GEOTILE_GRID:
                    fieldCollectors.put(
                        entry.getKey(),
                        new CompositeBucketsChangeCollector.GeoTileFieldCollector(
                            entry.getValue().getField(),
                            entry.getKey(),
                            entry.getValue().getMissingBucket()
                        )
                    );
                    break;
                default:
                    throw new IllegalArgumentException("unknown type");
            }
        }
        return fieldCollectors;
    }

}
