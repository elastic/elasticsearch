/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to validate a specific query.
 * <p>
 * The request requires the query to be set using {@link #query(QueryBuilder)}
 */
public final class ValidateQueryRequest extends BroadcastRequest<ValidateQueryRequest> implements ToXContentObject {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private QueryBuilder query = new MatchAllQueryBuilder();

    private boolean explain;
    private boolean rewrite;
    private boolean allShards;
    @Nullable
    private String routing;
    @Nullable
    private String searchSlice;
    private boolean routingFromSlice;

    long nowInMillis;

    public ValidateQueryRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public ValidateQueryRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        explain = in.readBoolean();
        rewrite = in.readBoolean();
        allShards = in.readBoolean();
        if (in.getTransportVersion().supports(SliceIndexing.VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION)) {
            routing = in.readOptionalString();
            searchSlice = in.readOptionalString();
            routingFromSlice = in.readBoolean();
        } else {
            routing = null;
            searchSlice = null;
            routingFromSlice = false;
        }
    }

    /**
     * Constructs a new validate request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public ValidateQueryRequest(String... indices) {
        super(indices);
        indicesOptions(DEFAULT_INDICES_OPTIONS);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (query == null) {
            validationException = ValidateActions.addValidationError("query cannot be null", validationException);
        }
        return validationException;
    }

    /**
     * The query to validate.
     */
    public QueryBuilder query() {
        return query;
    }

    public ValidateQueryRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    /**
     * Indicate if detailed information about query is requested
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * Indicates if detailed information about query is requested
     */
    public boolean explain() {
        return explain;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public void rewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public boolean rewrite() {
        return rewrite;
    }

    /**
     * Indicates whether the query should be validated on all shards instead of one random shard
     */
    public void allShards(boolean allShards) {
        this.allShards = allShards;
    }

    /**
     * Indicates whether the query should be validated on all shards instead of one random shard
     */
    public boolean allShards() {
        return allShards;
    }

    @Nullable
    public String routing() {
        return routing;
    }

    public ValidateQueryRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public ValidateQueryRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    @Nullable
    public String searchSlice() {
        return searchSlice;
    }

    public ValidateQueryRequest searchSlice(@Nullable String searchSlice) {
        this.searchSlice = searchSlice;
        if (searchSlice == null) {
            if (routingFromSlice) {
                this.routing = null;
            }
            this.routingFromSlice = false;
        } else {
            this.routingFromSlice = true;
            this.routing = SliceIndexing.SLICE_ALL.equals(searchSlice) ? null : searchSlice;
        }
        return this;
    }

    public boolean isRoutingFromSlice() {
        return routingFromSlice;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        out.writeBoolean(allShards);
        if (out.getTransportVersion().supports(SliceIndexing.VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION)) {
            out.writeOptionalString(routing);
            out.writeOptionalString(searchSlice);
            out.writeBoolean(routingFromSlice);
        }
    }

    @Override
    public String toString() {
        return "["
            + Arrays.toString(indices)
            + "] query["
            + query
            + "], explain:"
            + explain
            + ", rewrite:"
            + rewrite
            + ", all_shards:"
            + allShards
            + ", routing:"
            + routing
            + ", _slice:"
            + searchSlice;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("query");
        query.toXContent(builder, params);
        return builder.endObject();
    }
}
