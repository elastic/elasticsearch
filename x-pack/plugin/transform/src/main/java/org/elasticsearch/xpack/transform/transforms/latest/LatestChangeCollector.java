/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * {@link Function.ChangeCollector} implementation for the latest function.
 * Uses a two-phase approach to correctly handle the case where the sort field and sync.time.field
 * don't increase monotonically together (gh#90643).
 */
class LatestChangeCollector implements Function.ChangeCollector {

    static final String COMPOSITE_AGGREGATION_NAME = "_transform_latest_change_collector";

    private final String synchronizationField;
    private final List<String> uniqueKey;
    private final CompositeAggregationBuilder compositeAggregation;
    private final Map<String, Set<String>> changedKeyValues;
    private final Set<String> fieldsWithNullValues;

    LatestChangeCollector(String synchronizationField, List<String> uniqueKey) {
        this.synchronizationField = Objects.requireNonNull(synchronizationField);
        this.uniqueKey = Objects.requireNonNull(uniqueKey);
        this.compositeAggregation = createCompositeAggregation(uniqueKey);
        this.changedKeyValues = new HashMap<>();
        for (String field : uniqueKey) {
            changedKeyValues.put(field, new HashSet<>());
        }
        this.fieldsWithNullValues = new HashSet<>();
    }

    private static CompositeAggregationBuilder createCompositeAggregation(List<String> uniqueKey) {
        List<CompositeValuesSourceBuilder<?>> sources = uniqueKey.stream()
            .map(field -> new TermsValuesSourceBuilder(field).field(field).missingBucket(true))
            .collect(toList());
        return AggregationBuilders.composite(COMPOSITE_AGGREGATION_NAME, sources);
    }

    /**
     * Phase 1 (IDENTIFY_CHANGES): Build a composite aggregation over the checkpoint time window
     * to discover which unique_key values have new source documents.
     */
    @Override
    public SearchSourceBuilder buildChangesQuery(SearchSourceBuilder searchSourceBuilder, Map<String, Object> position, int pageSize) {
        compositeAggregation.size(pageSize);
        compositeAggregation.aggregateAfter(position);
        return searchSourceBuilder.size(0).aggregation(compositeAggregation);
    }

    /**
     * Phase 1 (IDENTIFY_CHANGES): Collect the unique_key values from the composite agg response;
     * these are the keys that need to be re-evaluated in phase 2.
     */
    @Override
    public Map<String, Object> processSearchResponse(SearchResponse searchResponse) {
        clearCollectedKeys();

        InternalAggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return null;
        }

        CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }

        for (CompositeAggregation.Bucket bucket : compositeAgg.getBuckets()) {
            for (String field : uniqueKey) {
                Object value = bucket.getKey().get(field);
                if (value != null) {
                    changedKeyValues.get(field).add(value.toString());
                } else {
                    fieldsWithNullValues.add(field);
                }
            }
        }

        return compositeAgg.afterKey();
    }

    /**
     * Phase 2 (APPLY_RESULTS): Build a filter so the main query searches only the collected
     * unique keys. The indexer applies sync_field &lt; nextCheckpoint separately, so the main
     * query sees ALL historical data for those keys and top_hits correctly picks the document
     * with the highest sort field value.
     */
    @Override
    public QueryBuilder buildFilterQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        if (uniqueKey.size() == 1) {
            String field = uniqueKey.get(0);
            return buildFieldFilter(field, changedKeyValues.get(field), fieldsWithNullValues.contains(field));
        }

        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        for (String field : uniqueKey) {
            QueryBuilder fieldFilter = buildFieldFilter(field, changedKeyValues.get(field), fieldsWithNullValues.contains(field));
            if (fieldFilter != null) {
                filterQuery.filter(fieldFilter);
            }
        }

        return filterQuery;
    }

    private static QueryBuilder buildFieldFilter(String field, Set<String> values, boolean includeNull) {
        if (includeNull) {
            QueryBuilder missingBucketQuery = new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(field));
            if (values.isEmpty()) {
                return missingBucketQuery;
            }
            return new BoolQueryBuilder().should(new TermsQueryBuilder(field, values)).should(missingBucketQuery);
        }

        if (values.isEmpty()) {
            return null;
        }
        return new TermsQueryBuilder(field, values);
    }

    @Override
    public Collection<String> getIndicesToQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        // gh#77329 optimization turned off
        return TransformCheckpoint.getChangedIndices(TransformCheckpoint.EMPTY, nextCheckpoint);
    }

    @Override
    public void clear() {
        clearCollectedKeys();
    }

    @Override
    public boolean isOptimized() {
        return true;
    }

    @Override
    public boolean queryForChanges() {
        return true;
    }

    private void clearCollectedKeys() {
        changedKeyValues.values().forEach(Set::clear);
        fieldsWithNullValues.clear();
    }
}
