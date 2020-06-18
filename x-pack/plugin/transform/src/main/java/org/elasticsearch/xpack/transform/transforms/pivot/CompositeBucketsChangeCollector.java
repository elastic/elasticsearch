/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.transform.transforms.ChangeCollector;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to collect bucket changes
 */
public class CompositeBucketsChangeCollector implements ChangeCollector {

    private final Map<String, FieldCollector> fieldCollectors;
    private final CompositeAggregationBuilder compositeAggregation;
    private Map<String, Object> afterKey = null;

    public interface FieldCollector {
        boolean collectChanges(Collection<? extends Bucket> buckets);

        AggregationBuilder aggregateChanges();

        QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp);

        void clear();
    }

    public static class TermsFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;
        private final Set<String> changedTerms;

        public TermsFieldCollector(final String sourceFieldName, final String targetFieldName) {
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
            this.changedTerms = new HashSet<>();
        }

        @Override
        public boolean collectChanges(Collection<? extends Bucket> buckets) {
            changedTerms.clear();

            for (Bucket b : buckets) {
                Object term = b.getKey().get(targetFieldName);
                if (term != null) {
                    changedTerms.add(term.toString());
                }
            }

            return true;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            if (changedTerms.isEmpty() == false) {
                return new TermsQueryBuilder(sourceFieldName, changedTerms);
            }
            return null;
        }

        @Override
        public void clear() {
            changedTerms.clear();
        }

        @Override
        public AggregationBuilder aggregateChanges() {
            return null;
        }
    }

    public static class DateHistogramFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;
        private final boolean isSynchronizationField;
        private final Rounding.Prepared rounding;

        public DateHistogramFieldCollector(
            final String sourceFieldName,
            final String targetFieldName,
            final Rounding.Prepared rounding,
            final boolean isSynchronizationField
        ) {
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
            this.rounding = rounding;
            this.isSynchronizationField = isSynchronizationField;
        }

        @Override
        public boolean collectChanges(Collection<? extends Bucket> buckets) {
            // todo: implementation for isSynchronizationField == false
            return false;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            if (isSynchronizationField && lastCheckpointTimestamp > 0) {
                return new RangeQueryBuilder(sourceFieldName).gte(rounding.round(lastCheckpointTimestamp)).format("epoch_millis");
            }

            // todo: implementation for isSynchronizationField == false

            return null;
        }

        @Override
        public void clear() {}

        @Override
        public AggregationBuilder aggregateChanges() {
            return null;
        }

    }

    public static class HistogramFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;

        public HistogramFieldCollector(final String sourceFieldName, final String targetFieldName) {
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
        }

        @Override
        public boolean collectChanges(Collection<? extends Bucket> buckets) {
            return false;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            return null;
        }

        @Override
        public void clear() {}

        @Override
        public AggregationBuilder aggregateChanges() {
            return null;
        }
    }

    public static class GeoTileFieldCollector implements FieldCollector {

        private final String sourceFieldName;
        private final String targetFieldName;
        private final Set<String> changedBuckets;

        public GeoTileFieldCollector(final String sourceFieldName, final String targetFieldName) {
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
            this.changedBuckets = new HashSet<>();
        }

        @Override
        public boolean collectChanges(Collection<? extends Bucket> buckets) {
            changedBuckets.clear();

            for (Bucket b : buckets) {
                Object bucket = b.getKey().get(targetFieldName);
                if (bucket != null) {
                    changedBuckets.add(bucket.toString());
                }
            }

            return true;
        }

        @Override
        public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
            // todo: this limited by indices.query.bool.max_clause_count, default 1024 which is lower than the maximum page size
            if (changedBuckets != null && changedBuckets.isEmpty() == false) {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                changedBuckets.stream().map(GeoTileUtils::toBoundingBox).map(this::toGeoQuery).forEach(boolQueryBuilder::should);
                return boolQueryBuilder;
            }
            return null;
        }

        @Override
        public void clear() {}

        @Override
        public AggregationBuilder aggregateChanges() {
            return null;
        }

        private GeoBoundingBoxQueryBuilder toGeoQuery(Rectangle rectangle) {
            return QueryBuilders.geoBoundingBoxQuery(sourceFieldName)
                .setCorners(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                );
        }
    }

    public CompositeBucketsChangeCollector(CompositeAggregationBuilder compositeAggregation, Map<String, FieldCollector> fieldCollectors) {
        this.compositeAggregation = compositeAggregation;
        this.fieldCollectors = fieldCollectors;
    }

    @Override
    public boolean collectChanges(final SearchResponse searchResponse) {
        final Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            // todo: log
            return true;
        }

        final CompositeAggregation agg = aggregations.get(compositeAggregation.getName());

        Collection<? extends Bucket> buckets = agg.getBuckets();
        afterKey = agg.afterKey();

        if (buckets.isEmpty()) {
            return true;
        }

        for (FieldCollector fieldCollector : fieldCollectors.values()) {
            fieldCollector.collectChanges(buckets);
        }

        return false;
    }

    @Override
    public QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp) {
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder();

        for (FieldCollector fieldCollector : fieldCollectors.values()) {
            QueryBuilder filter = fieldCollector.filterByChanges(lastCheckpointTimestamp, nextcheckpointTimestamp);
            if (filter != null) {
                filteredQuery.filter(filter);
            }
        }

        return filteredQuery;
    }

    @Override
    public SearchSourceBuilder buildChangesQuery(SearchSourceBuilder sourceBuilder, Map<String, Object> position, int pageSize) {

        CompositeAggregationBuilder changesAgg = this.compositeAggregation;
        changesAgg.size(pageSize).aggregateAfter(position);
        sourceBuilder.aggregation(changesAgg);
        sourceBuilder.size(0);
        for (FieldCollector fieldCollector : fieldCollectors.values()) {
            AggregationBuilder aggregationForField = fieldCollector.aggregateChanges();

            if (aggregationForField != null) {
                sourceBuilder.aggregation(aggregationForField);
            }
        }

        return sourceBuilder;
    }

    @Override
    public void clear() {
        fieldCollectors.forEach((k, c) -> c.clear());
    }

    @Override
    public Map<String, Object> getBucketPosition() {
        return afterKey;
    }

}
