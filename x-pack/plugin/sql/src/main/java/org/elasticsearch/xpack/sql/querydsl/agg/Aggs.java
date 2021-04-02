/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;

/**
 * SQL Aggregations associated with a query.
 *
 * This class maps the SQL GroupBy's (and co) to ES composite agg.
 * While the composite agg doesn't require a dedicated structure, for folding purposes, this structure
 * tracks the relationship between each key and its sub-aggs or pipelines.
 *
 * Since sub-aggs can only refer to their group key and these are on the root-level, the tree can have at most
 * 2 levels - the grouping and its sub-aggs.
 *
 * In case no group is specified (which maps to the default group in SQL), due to ES nature a 'dummy' filter agg
 * is used.
 */
public class Aggs {

    public static final String ROOT_GROUP_NAME = "groupby";

    public static final GroupByKey IMPLICIT_GROUP_KEY = new GroupByKey(ROOT_GROUP_NAME, AggSource.of(StringUtils.EMPTY), null) {

        @Override
        public CompositeValuesSourceBuilder<?> createSourceBuilder() {
            throw new SqlIllegalArgumentException("Default group does not translate to an aggregation");
        }

        @Override
        protected GroupByKey copy(String id, AggSource source, Direction direction) {
            return this;
        }
    };

    public static final Aggs EMPTY = new Aggs(emptyList(), emptyList(), emptyList());

    private final List<GroupByKey> groups;
    private final List<LeafAgg> simpleAggs;
    private final List<PipelineAgg> pipelineAggs;

    public Aggs(List<GroupByKey> groups, List<LeafAgg> simpleAggs, List<PipelineAgg> pipelineAggs) {
        this.groups = groups;

        this.simpleAggs = simpleAggs;
        this.pipelineAggs = pipelineAggs;
    }

    public List<GroupByKey> groups() {
        return groups;
    }

    public AggregationBuilder asAggBuilder() {
        AggregationBuilder rootGroup = null;

        if (groups.isEmpty() && simpleAggs.isEmpty()) {
            return null;
        }

        // if there's a group, move everything under the composite agg
        if (groups.isEmpty() == false) {
            List<CompositeValuesSourceBuilder<?>> keys = new ArrayList<>(groups.size());
            // first iterate to compute the sources
            for (GroupByKey key : groups) {
                keys.add(key.asValueSource());
            }

            rootGroup = new CompositeAggregationBuilder(ROOT_GROUP_NAME, keys);

        } else {
            rootGroup = new FiltersAggregationBuilder(ROOT_GROUP_NAME, matchAllQuery());
        }

        for (LeafAgg agg : simpleAggs) {
            rootGroup.subAggregation(agg.toBuilder());
        }

        for (PipelineAgg agg : pipelineAggs) {
            rootGroup.subAggregation(agg.toBuilder());
        }

        return rootGroup;
    }

    public boolean useImplicitGroupBy() {
        return groups.isEmpty();
    }

    public Aggs addGroups(Collection<GroupByKey> groups) {
        return new Aggs(combine(this.groups, groups), simpleAggs, pipelineAggs);
    }

    public Aggs addAgg(LeafAgg agg) {
        if (simpleAggs.contains(agg)) {
            return this;
        }
        return new Aggs(groups, combine(simpleAggs, agg), pipelineAggs);
    }

    public Aggs addAgg(PipelineAgg pipelineAgg) {
        return new Aggs(groups, simpleAggs, combine(pipelineAggs, pipelineAgg));
    }

    public GroupByKey findGroupForAgg(String groupOrAggId) {
        for (GroupByKey group : this.groups) {
            if (groupOrAggId.equals(group.id())) {
                return group;
            }
        }

        // maybe it's the default group agg ?
        for (Agg agg : simpleAggs) {
            if (groupOrAggId.equals(agg.id())) {
                return IMPLICIT_GROUP_KEY;
            }
        }

        return null;
    }

    public Aggs updateGroup(GroupByKey group) {
        List<GroupByKey> groups = new ArrayList<>(this.groups);
        for (int i = 0; i < groups.size(); i++) {
            GroupByKey g = groups.get(i);
            if (group.id().equals(g.id())) {
                groups.set(i, group);
                return with(groups);
            }
        }
        throw new SqlIllegalArgumentException("Could not find group named {}", group.id());
    }

    public Aggs with(List<GroupByKey> groups) {
        return new Aggs(groups, simpleAggs, pipelineAggs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups, simpleAggs, pipelineAggs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Aggs other = (Aggs) obj;
        return Objects.equals(groups, other.groups)
                && Objects.equals(simpleAggs, other.simpleAggs)
                && Objects.equals(pipelineAggs, other.pipelineAggs);

    }
}
