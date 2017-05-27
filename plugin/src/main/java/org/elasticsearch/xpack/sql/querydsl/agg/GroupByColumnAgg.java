/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xpack.sql.querydsl.container.Sort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

public class GroupByColumnAgg extends GroupingAgg {

    public GroupByColumnAgg(String id, String propertyPath, String fieldName) {
        this(id, propertyPath, fieldName, emptyList(), emptyList(), emptyMap());
    }

    public GroupByColumnAgg(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order) {
        super(id, propertyPath, fieldName, subAggs, subPipelines, order);
    }

    @Override
    protected AggregationBuilder toGroupingAgg() {
        // TODO: the size should be configurable
        TermsAggregationBuilder terms = terms(id()).field(fieldName()).size(100);

        List<BucketOrder> termOrders = emptyList();
        if (!order().isEmpty()) {
            termOrders = new ArrayList<>();
            for (Entry<String, Sort.Direction> entry : order().entrySet()) {
                String key = entry.getKey();
                boolean asc = entry.getValue() == Direction.ASC;
                BucketOrder o = null;
                // special cases
                if (GROUP_KEY_SORTING.equals(key)) {
                    o = BucketOrder.key(asc);
                }
                else if (GROUP_COUNT_SORTING.equals(key)) {
                    o = BucketOrder.count(asc);
                }
                else {
                    o = BucketOrder.aggregation(key, asc);
                }
                termOrders.add(o);
            }
            terms.order(termOrders);
        }
        return terms;
    }

    @Override
    protected GroupByColumnAgg clone(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order) {
        return new GroupByColumnAgg(id, propertyPath, fieldName, subAggs, subPipelines, order);
    }
}