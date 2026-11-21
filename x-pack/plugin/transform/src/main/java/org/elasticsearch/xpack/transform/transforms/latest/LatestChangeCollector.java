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
import org.elasticsearch.index.query.TermQueryBuilder;
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

import java.util.ArrayList;
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
    /**
     * Most boolean clauses the exact tuple filter may use before falling back to the cross-product filter.
     * The default indices.query.bool.max_clause_count is 4096 and is counted over the whole query, which
     * also holds the source query and the sync range, so this stays at half of it.
     */
    static final int MAX_TUPLE_FILTER_CLAUSES = 2048;

    private final String synchronizationField;
    private final List<String> uniqueKey;
    private final CompositeAggregationBuilder compositeAggregation;
    private final Map<String, Set<String>> changedKeyValues;
    private final Set<String> fieldsWithNullValues;
    // Multi-field keys only: the exact changed tuples, in uniqueKey order; a null element is a missing bucket.
    private final List<List<String>> changedKeyTuples;

    LatestChangeCollector(String synchronizationField, List<String> uniqueKey) {
        this.synchronizationField = Objects.requireNonNull(synchronizationField);
        this.uniqueKey = Objects.requireNonNull(uniqueKey);
        this.compositeAggregation = createCompositeAggregation(uniqueKey);
        this.changedKeyValues = new HashMap<>();
        for (String field : uniqueKey) {
            changedKeyValues.put(field, new HashSet<>());
        }
        this.fieldsWithNullValues = new HashSet<>();
        this.changedKeyTuples = new ArrayList<>();
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
            List<String> tuple = new ArrayList<>(uniqueKey.size());
            for (String field : uniqueKey) {
                Object value = bucket.getKey().get(field);
                if (value != null) {
                    changedKeyValues.get(field).add(value.toString());
                } else {
                    fieldsWithNullValues.add(field);
                }
                tuple.add(value != null ? value.toString() : null);
            }
            if (uniqueKey.size() > 1) {
                changedKeyTuples.add(tuple);
            }
        }

        return compositeAgg.afterKey();
    }

    /**
     * Phase 2 (APPLY_RESULTS): Build a filter so the main query searches only the collected
     * unique keys. The indexer applies sync_field &lt; nextCheckpoint separately, so the main
     * query sees ALL historical data for those keys and top_hits correctly picks the document
     * with the highest sort field value.
     *
     * For a multi-field unique key the filter matches the changed tuples exactly. One terms
     * filter per field, ANDed, would match their cross product: with 3,000 changed hosts and
     * 200 changed users that is 600,000 candidate pairs for 5,000 changed ones, every unchanged
     * pair among them is rewritten, and the extra buckets page the phase-2 query several times
     * per round, each page re-running it over the full history.
     */
    @Override
    public QueryBuilder buildFilterQuery(TransformCheckpoint lastCheckpoint, TransformCheckpoint nextCheckpoint) {
        if (uniqueKey.size() == 1) {
            String field = uniqueKey.get(0);
            return buildFieldFilter(field, changedKeyValues.get(field), fieldsWithNullValues.contains(field));
        }
        if (changedKeyTuples.isEmpty()) {
            return null;
        }

        List<Integer> positions = new ArrayList<>(uniqueKey.size());
        for (int i = 0; i < uniqueKey.size(); i++) {
            positions.add(i);
        }
        QueryBuilder exactFilter = buildTupleFilter(positions, changedKeyTuples, new int[] { MAX_TUPLE_FILTER_CLAUSES });
        // null means the exact filter would need too many clauses, so use the looser one, which is always two per field
        return exactFilter != null ? exactFilter : buildCrossProductFilter();
    }

    /** One terms filter per field, ANDed: a superset of the changed tuples, correct but loose. */
    private QueryBuilder buildCrossProductFilter() {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        for (String field : uniqueKey) {
            QueryBuilder fieldFilter = buildFieldFilter(field, changedKeyValues.get(field), fieldsWithNullValues.contains(field));
            if (fieldFilter != null) {
                filterQuery.filter(fieldFilter);
            }
        }
        return filterQuery;
    }

    /**
     * Exact filter for a set of key tuples over the given field positions. Tuples are grouped by
     * the field with the fewest distinct values, so the clause count follows that field's
     * cardinality (about three per group) rather than one bool per tuple.
     *
     * @param clausesLeft single-element counter of boolean clauses still allowed, decremented as the filter is built
     * @return the filter, or null if it would need more than the allowed clauses
     */
    private QueryBuilder buildTupleFilter(List<Integer> positions, List<List<String>> tuples, int[] clausesLeft) {
        if (positions.size() == 1) {
            int position = positions.get(0);
            Set<String> values = new HashSet<>();
            boolean includeNull = false;
            for (List<String> tuple : tuples) {
                String value = tuple.get(position);
                if (value == null) {
                    includeNull = true;
                } else {
                    values.add(value);
                }
            }
            // a missing value adds a must_not, and with other values a should for each side as well
            clausesLeft[0] -= includeNull ? (values.isEmpty() ? 1 : 3) : 0;
            return clausesLeft[0] < 0 ? null : buildFieldFilter(uniqueKey.get(position), values, includeNull);
        }

        int pivot = positions.get(0);
        long fewest = Long.MAX_VALUE;
        for (int position : positions) {
            long distinct = tuples.stream().map(tuple -> tuple.get(position)).distinct().count();
            if (distinct < fewest) {
                fewest = distinct;
                pivot = position;
            }
        }

        final int pivotPosition = pivot;
        Map<String, List<List<String>>> groups = new HashMap<>();
        for (List<String> tuple : tuples) {
            groups.computeIfAbsent(tuple.get(pivotPosition), value -> new ArrayList<>()).add(tuple);
        }
        List<Integer> rest = positions.stream().filter(position -> position != pivotPosition).collect(toList());
        String pivotField = uniqueKey.get(pivotPosition);

        BoolQueryBuilder anyGroup = new BoolQueryBuilder().minimumShouldMatch(1);
        for (Map.Entry<String, List<List<String>>> group : groups.entrySet()) {
            // the should in the parent, the pivot filter and the rest filter, plus a must_not for a missing pivot
            clausesLeft[0] -= group.getKey() == null ? 4 : 3;
            if (clausesLeft[0] < 0) {
                return null;
            }
            QueryBuilder pivotFilter = group.getKey() == null
                ? new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(pivotField))
                : new TermQueryBuilder(pivotField, group.getKey());
            QueryBuilder restFilter = buildTupleFilter(rest, group.getValue(), clausesLeft);
            if (restFilter == null) {
                return null;
            }
            anyGroup.should(new BoolQueryBuilder().filter(pivotFilter).filter(restFilter));
        }
        return anyGroup.should().size() == 1 ? anyGroup.should().get(0) : anyGroup;
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
        changedKeyTuples.clear();
    }
}
