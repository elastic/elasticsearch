/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@code RankContextBuilder} is used as a base class to manage input, parsing,
 * and subsequent generation of appropriate contexts for handling searches that
 * require multiple queries for global rank relevance.
 *
 * This class contains a {@code List<QueryBuilder>}s, size, and from members that
 * sub-classes use to execute queries to generate ranking results. These are not part
 * of ranking input, but instead generated from their respective values within a search
 * query. We cannot generate the list of queries until after any kNN queries are executed.
 */
public abstract class RankContextBuilder implements VersionedNamedWriteable, ToXContent {

    protected final List<QueryBuilder> queryBuilders;
    protected int size = SearchService.DEFAULT_SIZE;
    protected int from = SearchService.DEFAULT_FROM;

    public RankContextBuilder() {
        queryBuilders = new ArrayList<>();
    }

    public RankContextBuilder(StreamInput in) throws IOException {
        queryBuilders = in.readNamedWriteableList(QueryBuilder.class);
        size = in.readVInt();
        from = in.readVInt();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queryBuilders);
        out.writeVInt(size);
        out.writeVInt(from);
    }

    public abstract ActionRequestValidationException validate(ActionRequestValidationException exception, SearchSourceBuilder source);

    public List<QueryBuilder> queryBuilders() {
        return queryBuilders;
    }

    public RankContextBuilder size(int size) {
        this.size = size == -1 ? SearchService.DEFAULT_SIZE : size;
        return this;
    }

    public RankContextBuilder from(int from) {
        this.from = from == -1 ? SearchService.DEFAULT_FROM : from;
        return this;
    }

    /**
     * Generates a shallow copy for creating a new {@link SearchSourceBuilder}
     * after kNN queries are executed during the DFS phase. This allows us
     * to create kNN queries that are only relevant to a single shard.
     */
    public RankContextBuilder shallowCopy() {
        RankContextBuilder rankContextBuilder = subShallowCopy();
        rankContextBuilder.queryBuilders.addAll(this.queryBuilders);
        rankContextBuilder.size = this.size;
        rankContextBuilder.from = this.from;
        return rankContextBuilder;
    }

    public abstract RankContextBuilder subShallowCopy();

    /**
     * Generates the query used for aggregations and suggesters.
     */
    public abstract QueryBuilder searchQuery();

    /**
     * Generates a context used to execute required searches on the shard.
     */
    public abstract RankShardContext build(SearchExecutionContext searchExecutionContext) throws IOException;

    /**
     * Generates a context used to perform global ranking on the coordinator.
     */
    public abstract RankContext build();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankContextBuilder that = (RankContextBuilder) o;
        return size == that.size && from == that.from && Objects.equals(queryBuilders, that.queryBuilders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilders, size, from);
    }

    @Override
    public String toString() {
        return "RankContextBuilder{" + "queryBuilders=" + queryBuilders + ", size=" + size + ", from=" + from + '}';
    }
}
