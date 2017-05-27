/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import static java.util.Collections.emptyList;

import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

// Aggregations associated with a query.
//
// This maps the SQL target namely:
// - aggregations are either at the root (default or nameless group) or belong to a named group
// - groups are chained in a linear fashion (group1 -> group2 -> group3)
//
// Aggregations are of two types - concrete metric aggs or pipeline/filtering aggs
//
// Pipeline Notes:
// 
// As pipeline aggs can refers to aggs across different groups (different tree levels)
// one needs to pay attention attention before adding them to properly find the lowest level and
// make everything relative to that.
//
// For example sum > 100 && count < 10 might imply sum referring on level 2 while count is on level 3
// In this case, the pipeline would be added to group/level 2 (and refer to sum) while count will be
// made relative to this group/level 3 namely group3>count

public class Aggs {

    private final List<GroupingAgg> groups;
    private final List<Agg> rootAggs;
    private final List<PipelineAgg> rootPipelineAggs;

    public Aggs() {
        this(emptyList(), emptyList(), emptyList());
    }

    public Aggs(List<Agg> rootAggs, List<PipelineAgg> rootPipelineAggs, List<GroupingAgg> groups) {
        this.rootAggs = rootAggs;
        this.rootPipelineAggs = rootPipelineAggs;
        this.groups = groups;
    }

    public List<GroupingAgg> groups() {
        return groups;
    }

    public List<AggregationBuilder> asAggBuilders() {
        List<AggregationBuilder> aggBuilders = new ArrayList<>(rootAggs.size() + 1);

        for (Agg agg : rootAggs) {
            aggBuilders.add(agg.toBuilder());
        }

        if (!groups.isEmpty()) {
            AggregationBuilder root = null, last = null;
            for (GroupingAgg groupBy : groups) {
                AggregationBuilder builder = groupBy.toBuilder();
                if (root == null) {
                    root = builder;
                }
                // chain each group
                if (last != null) {
                    last.subAggregation(builder);
                }
                last = builder;
            }

            aggBuilders.add(root);
        }

        return aggBuilders;
    }

    public List<PipelineAggregationBuilder> asPipelineBuilders() {
        List<PipelineAggregationBuilder> aggBuilders = new ArrayList<>(rootPipelineAggs.size() + 1);

        for (PipelineAgg agg : rootPipelineAggs) {
            aggBuilders.add(agg.toBuilder());
        }

        return aggBuilders;
    }

    public Aggs addGroups(Collection<GroupingAgg> groups) {
        return new Aggs(rootAggs, rootPipelineAggs, combine(this.groups, groups));
    }

    public Aggs addAgg(Agg agg) {
        if (agg instanceof GroupingAgg) {
            GroupingAgg group = (GroupingAgg) agg;
            return with(combine(this.groups, group));
        }
        if (agg instanceof LeafAgg) {
            LeafAgg leaf = (LeafAgg) agg;
            if (groups.isEmpty()) {
                return new Aggs(combine(rootAggs, leaf), rootPipelineAggs, this.groups);
            }
            else {
                // copy the groups and replace the tail
                List<GroupingAgg> groups = new ArrayList<>(this.groups);
                GroupingAgg tail = groups.get(groups.size() - 1);
                groups.set(groups.size() - 1, tail.withAggs(combine(tail.subAggs(), leaf)));
                return with(groups);
            }
        }

        throw new SqlIllegalArgumentException("Does not know how to handle type %s", agg);
    }
    
    public Aggs addAgg(PipelineAgg pipelineAgg) {
        if (groups.isEmpty()) {
            return new Aggs(rootAggs, combine(rootPipelineAggs, pipelineAgg), this.groups);
        }
        else {
            // copy the groups and replace the tail
            List<GroupingAgg> groups = new ArrayList<>(this.groups);
            GroupingAgg tail = groups.get(groups.size() - 1);
            groups.set(groups.size() - 1, tail.withPipelines(combine(tail.subPipelines(), pipelineAgg)));
            return with(groups);
        }
    }


    public Aggs addAgg(String groupId, LeafAgg child) {
        // it's a root agg
        if (groupId == null) {
            return new Aggs(combine(rootAggs, child), rootPipelineAggs, groups());
        }

        List<GroupingAgg> groups = new ArrayList<>(this.groups);

        for (int i = 0; i < groups.size(); i++) {
            GroupingAgg group = groups.get(i);
            if (groupId.equals(group.id())) {
                groups.set(i, group.withAggs(combine(group.subAggs(), child)));
                return with(groups);
            }
        }
        throw new SqlIllegalArgumentException("Could not find group named %s", groupId);
    }
    
    public Aggs addAgg(String groupId, PipelineAgg child) {
        // it's a root agg
        if (groupId == null) {
            return new Aggs(rootAggs, combine(rootPipelineAggs, child), groups());
        }

        List<GroupingAgg> groups = new ArrayList<>(this.groups);

        for (int i = 0; i < groups.size(); i++) {
            GroupingAgg group = groups.get(i);
            if (groupId.equals(group.id())) {
                groups.set(i, group.withPipelines(combine(group.subPipelines(), child)));
                return with(groups);
            }
        }
        throw new SqlIllegalArgumentException("Could not find group named %s", groupId);
    }

    public GroupingAgg findGroupForAgg(String groupOrAggId) {
        for (GroupingAgg group : this.groups) {
            if (groupOrAggId.equals(group.id())) {
                return group;
            }
            for (LeafAgg subAgg : group.subAggs()) {
                if (groupOrAggId.equals(subAgg.id())) {
                    return group;
                }
            }
        }

        // is it maybe a root agg
        for (Agg agg : rootAggs) {
            if (groupOrAggId.equals(agg.id())) {
                return GroupingAgg.DEFAULT_GROUP;
            }
        }

        return null;
    }

    public Aggs updateGroup(GroupingAgg group) {
        List<GroupingAgg> groups = new ArrayList<>(this.groups);
        for (int i = 0; i < groups.size(); i++) {
            GroupingAgg g = groups.get(i);
            if (group.id().equals(g.id())) {
                groups.set(i, group);
                return with(groups);
            }
        }
        throw new SqlIllegalArgumentException("Could not find group named %s", group.id());
    }
    
    private Aggs with(List<GroupingAgg> groups) {
        return new Aggs(rootAggs, rootPipelineAggs, groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rootAggs, rootPipelineAggs, groups);
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
        return Objects.equals(rootAggs, other.rootAggs) 
                && Objects.equals(rootPipelineAggs, other.rootPipelineAggs)
                && Objects.equals(groups, other.groups);
    }
}