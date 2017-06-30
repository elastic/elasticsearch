/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;

import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class GroupingAgg extends Agg {

    protected static final String GROUP_KEY_SORTING = "_key";
    protected static final String GROUP_COUNT_SORTING = "_count";

    public static final GroupingAgg DEFAULT_GROUP = new GroupingAgg(EMPTY, EMPTY, EMPTY, emptyList(), emptyList(), null) {

        @Override
        protected AggregationBuilder toGroupingAgg() {
            throw new SqlIllegalArgumentException("Default group cannot build aggregations");
        }

        @Override
        protected GroupingAgg clone(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order) {
            throw new SqlIllegalArgumentException("Default group cannot be cloned");
        }
    };

    private final List<LeafAgg> subAggs;
    private final List<PipelineAgg> subPipelines;
    private final Map<String, Direction> order;

    GroupingAgg(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order) {
        super(id, propertyPath, fieldName);
        this.subAggs = subAggs;
        this.subPipelines = subPipelines;
        this.order = order;
    }

    public List<LeafAgg> subAggs() {
        return subAggs;
    }

    public List<PipelineAgg> subPipelines() {
        return subPipelines;
    }
    
    public Map<String, Direction> order() {
        return order;
    }

    @Override
    AggregationBuilder toBuilder() {
        AggregationBuilder groupingAgg = toGroupingAgg();
        for (LeafAgg leafAgg : subAggs) {
            groupingAgg.subAggregation(leafAgg.toBuilder());
        }
        for (PipelineAgg aggFilter : subPipelines) {
            groupingAgg.subAggregation(aggFilter.toBuilder());
        }
        return groupingAgg;
    }

    protected abstract AggregationBuilder toGroupingAgg();


    public GroupingAgg withAggs(List<LeafAgg> subAggs) {
        return clone(id(), propertyPath(), fieldName(), subAggs, subPipelines, order);
    }

    public GroupingAgg withPipelines(List<PipelineAgg> subPipelines) {
        return clone(id(), propertyPath(), fieldName(), subAggs, subPipelines, order);
    }

    public GroupingAgg with(String id) {
        return Objects.equals(id(), id) ? this : clone(id, propertyPath(), fieldName(), subAggs, subPipelines, order);
    }

    public GroupingAgg with(Direction order) {
        return with(GROUP_KEY_SORTING, order);
    }

    public GroupingAgg with(String leafAggId, Direction order) {
        return Objects.equals(this.order.get(leafAggId), order) ? this : clone(id(), propertyPath(), fieldName(), subAggs, subPipelines, combine(this.order, singletonMap(leafAggId, order)));
    }

    // NOCOMMIT clone is a scary name.
    protected abstract GroupingAgg clone(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order);

    @Override
    public int hashCode() {
        return Objects.hash(order, id(), propertyPath(), fieldName(), subAggs(), subPipelines());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        GroupByColumnAgg other = (GroupByColumnAgg) obj;
        return Objects.equals(id(), other.id()) 
                && Objects.equals(propertyPath(), other.propertyPath())
                && Objects.equals(fieldName(), other.fieldName())
                && Objects.equals(subAggs(), other.subAggs())
                && Objects.equals(subPipelines(), other.subPipelines());
    }
    
    @Override
    public String toString() {
        return super.toString() + "=" + subAggs() + "|" + subPipelines();
    }
}