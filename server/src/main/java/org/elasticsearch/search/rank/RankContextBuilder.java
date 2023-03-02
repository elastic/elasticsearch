/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

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
public abstract class RankContextBuilder<RCB extends RankContextBuilder<RCB>> implements VersionedNamedWriteable, ToXContent {

    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    public static final int DEFAULT_WINDOW_SIZE = SearchService.DEFAULT_SIZE;

    protected int windowSize = DEFAULT_WINDOW_SIZE;

    protected final List<QueryBuilder> queryBuilders;
    protected int size = SearchService.DEFAULT_SIZE;
    protected int from = SearchService.DEFAULT_FROM;

    public RankContextBuilder() {
        queryBuilders = new ArrayList<>();
    }

    public RankContextBuilder(StreamInput in) throws IOException {
        windowSize = in.readVInt();
        queryBuilders = in.readNamedWriteableList(QueryBuilder.class);
        size = in.readVInt();
        from = in.readVInt();
    }

    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(windowSize);
        out.writeNamedWriteableList(queryBuilders);
        out.writeVInt(size);
        out.writeVInt(from);
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getWriteableName());
        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        doXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Allows additional validation as part of {@link SearchRequest#validate} based on subclass rank parameters.
     */
    public abstract ActionRequestValidationException validate(
        ActionRequestValidationException validationException,
        SearchSourceBuilder source
    );

    @SuppressWarnings("unchecked")
    public RCB windowSize(int windowSize) {
        this.windowSize = windowSize;
        return (RCB) this;
    }

    public int windowSize() {
        return windowSize;
    }

    public List<QueryBuilder> queryBuilders() {
        return queryBuilders;
    }

    @SuppressWarnings("unchecked")
    public RCB size(int size) {
        this.size = size == -1 ? SearchService.DEFAULT_SIZE : size;
        return (RCB) this;
    }

    public int size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    public RCB from(int from) {
        this.from = from == -1 ? SearchService.DEFAULT_FROM : from;
        return (RCB) this;
    }

    public int from() {
        return from;
    }

    /**
     * Generates a shallow copy for creating a new {@link SearchSourceBuilder}
     * after kNN queries are executed during the DFS phase. This allows us
     * to create kNN queries that are only relevant to a single shard.
     */
    public RCB shallowCopy() {
        RCB rankContextBuilder = subShallowCopy();
        rankContextBuilder.windowSize = this.windowSize;
        rankContextBuilder.queryBuilders.addAll(this.queryBuilders);
        rankContextBuilder.size = this.size;
        rankContextBuilder.from = this.from;
        return rankContextBuilder;
    }

    public abstract RCB subShallowCopy();

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
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        RCB other = (RCB) obj;
        return Objects.equals(windowSize, other.windowSize)
            && Objects.equals(queryBuilders, other.queryBuilders)
            && Objects.equals(size, other.size)
            && Objects.equals(from, other.from)
            && doEquals(other);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(RCB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), windowSize, queryBuilders, size, from, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString() {
        return toString(EMPTY_PARAMS);
    }

    public String toString(Params params) {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, params, true).utf8ToString();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}
