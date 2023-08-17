/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.RuntimeUtils;
import org.elasticsearch.xpack.eql.expression.OptionalMissingAttribute;
import org.elasticsearch.xpack.eql.expression.OptionalResolvedAttribute;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.eql.execution.ExecutionUtils.copySource;

public class SampleQueryRequest implements QueryRequest {

    public static final String COMPOSITE_AGG_NAME = "keys";
    private SearchSourceBuilder searchSource;
    private final List<String> keys; // the name of the join keys
    private final List<Attribute> keyFields;
    private CompositeAggregationBuilder agg;
    private List<QueryBuilder> multipleKeyFilters;
    private List<QueryBuilder> singleKeyPairFilters;
    private final int fetchSize;

    public SampleQueryRequest(QueryRequest original, List<String> keyNames, List<Attribute> keyFields, int fetchSize) {
        this.searchSource = original.searchSource();
        this.keys = keyNames;
        this.keyFields = keyFields;
        this.fetchSize = fetchSize;
    }

    @Override
    public SearchSourceBuilder searchSource() {
        return searchSource;
    }

    @Override
    public void nextAfter(Ordinal ordinal) {}

    public void nextAfter(Map<String, Object> afterKeys) {
        agg.aggregateAfter(afterKeys);
    }

    public List<String> keys() {
        return keys;
    }

    /**
     * Sets keys / terms to filter on in an intermediate stage filtering.
     * Can be removed through null.
     */
    public void multipleKeyPairs(List<Map<String, Object>> values, List<String> previousCriterionKeys) {
        assert previousCriterionKeys != null && previousCriterionKeys.size() == keys.size();

        List<QueryBuilder> newFilters;
        if (values.isEmpty()) {
            // no keys have been specified and none have been set
            if (CollectionUtils.isEmpty(multipleKeyFilters)) {
                return;
            }
            newFilters = emptyList();
        } else {
            BoolQueryBuilder orKeys = boolQuery();
            newFilters = Collections.singletonList(orKeys);

            for (Map<String, Object> bucket : values) {
                BoolQueryBuilder joinKeyBoolQuery = null;
                // the list of keys order is important because a key on one position corresponds to another key on the same
                // position from another query. For example, [host, os] corresponds to [hostname, op_sys].
                for (int i = 0; i < keys.size(); i++) {
                    String key = keys.get(i);
                    // build a bool must for the key of this criterion, but using the value of the previous criterion results
                    Object value = bucket.get(previousCriterionKeys.get(i));

                    if (value != null || isOptionalAttribute(keyFields.get(i))) {
                        if (joinKeyBoolQuery == null) {
                            joinKeyBoolQuery = boolQuery();
                        }
                        if (value != null) {
                            joinKeyBoolQuery.must(termQuery(key, value));
                        } else {
                            /*
                             * Joining on null values can generate technically valid results, but difficult o understand by users.
                             * For example,
                             * sample by host [any where bool == true] by os [any where uptime > 0] by os [any where port > 100] by op_sys
                             *
                             * If we would allow "null" values as valid join keys, it is possible to get a match on documents that do not
                             * have a value for "op_sys" in some indices (but have a value on "os") and other documents that do not have
                             * a value for "os" (but have a value in "op_sys").
                             *
                             * Result for the above query:
                             * "join_keys": ["doom",null]
                             * "events":
                             *    [{"_index":"test2","_id": "6","host": "doom","port": 65123,"bool": true,"op_sys": "redhat"}
                             *    {"_index": "test2","_id": "7","host": "doom","uptime": 15,"port": 1234,"bool": true,"op_sys": "redhat"}
                             *    {"_index": "test1","_id": "1","host": "doom","uptime": 0,"port": 1234,"os": "win10"}]
                             */
                            joinKeyBoolQuery.mustNot(existsQuery(key));
                        }
                    }
                }

                if (joinKeyBoolQuery != null) {
                    orKeys.should(joinKeyBoolQuery);
                }
            }
        }

        RuntimeUtils.replaceFilter(multipleKeyFilters, newFilters, searchSource);
        multipleKeyFilters = newFilters;
    }

    /**
     * Sets keys / terms to filter on in the final stage filtering (where actual events are gathered).
     * Can be removed through null.
     */
    public void singleKeyPair(final List<Object> compositeKeyValues, int maxStages, int maxSamplesPerKey) {
        List<QueryBuilder> newFilters = new ArrayList<>();
        if (compositeKeyValues.isEmpty()) {
            // no keys have been specified and none have been set
            if (CollectionUtils.isEmpty(singleKeyPairFilters)) {
                return;
            }
            newFilters = emptyList();
        } else {
            Object value;
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                value = compositeKeyValues.get(i);
                if (value != null) {
                    newFilters.add(termQuery(key, value));
                } else if (isOptionalAttribute(keyFields.get(i))) {
                    newFilters.add(boolQuery().mustNot(existsQuery(key)));
                }
            }
        }

        SearchSourceBuilder newSource = copySource(searchSource);
        RuntimeUtils.replaceFilter(singleKeyPairFilters, newFilters, newSource);
        // ask for the minimum needed to get at least N samplese per key
        int minResultsNeeded = maxStages + maxSamplesPerKey - 1;
        newSource.size(minResultsNeeded)
            .terminateAfter(minResultsNeeded) // no need to ask for more from each shard since we don't need sorting or more docs
            .fetchSource(FetchSourceContext.DO_NOT_FETCH_SOURCE) // we'll get the source in a separate step
            .trackTotalHits(false)
            .trackScores(false);
        singleKeyPairFilters = newFilters;
        searchSource = newSource;
    }

    public void withCompositeAggregation() {
        if (this.agg != null) {
            return;
        }

        List<CompositeValuesSourceBuilder<?>> compositeAggSources = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            Attribute field = keyFields.get(i);
            boolean isOptionalKey = isOptionalAttribute(field);
            compositeAggSources.add(new TermsValuesSourceBuilder(key).field(key).missingBucket(isOptionalKey));
        }
        agg = new CompositeAggregationBuilder(COMPOSITE_AGG_NAME, compositeAggSources);
        agg.size(fetchSize);
        searchSource.aggregation(agg);
    }

    private boolean isOptionalAttribute(Attribute a) {
        return a instanceof OptionalMissingAttribute || a instanceof OptionalResolvedAttribute;
    }

}
