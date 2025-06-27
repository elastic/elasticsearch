/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * A request to find the list of target shards that might match the query for the given target indices.
 */
public final class SearchShardsRequest extends LegacyActionRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    @Nullable
    private final QueryBuilder query;

    @Nullable
    private final String routing;
    @Nullable
    private final String preference;

    private final boolean allowPartialSearchResults;

    private final String clusterAlias;

    public SearchShardsRequest(
        String[] indices,
        IndicesOptions indicesOptions,
        QueryBuilder query,
        String routing,
        String preference,
        boolean allowPartialSearchResults,
        String clusterAlias
    ) {
        this.indices = indices;
        this.indicesOptions = indicesOptions;
        this.query = query;
        this.routing = routing;
        this.preference = preference;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.clusterAlias = clusterAlias;
    }

    public SearchShardsRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.query = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.routing = in.readOptionalString();
        this.preference = in.readOptionalString();
        this.allowPartialSearchResults = in.readBoolean();
        this.clusterAlias = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalNamedWriteable(query);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBoolean(allowPartialSearchResults);
        out.writeOptionalString(clusterAlias);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::description, parentTaskId, headers);
    }

    public String clusterAlias() {
        return clusterAlias;
    }

    public QueryBuilder query() {
        return query;
    }

    public String routing() {
        return routing;
    }

    public String preference() {
        return preference;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    private String description() {
        return "indices="
            + Arrays.toString(indices)
            + ", indicesOptions="
            + indicesOptions
            + ", query="
            + query
            + ", routing='"
            + routing
            + '\''
            + ", preference='"
            + preference
            + '\''
            + ", allowPartialSearchResults="
            + allowPartialSearchResults
            + ", clusterAlias="
            + clusterAlias;
    }

    @Override
    public String toString() {
        return "SearchShardsRequest{" + description() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardsRequest request = (SearchShardsRequest) o;
        return Arrays.equals(indices, request.indices)
            && Objects.equals(indicesOptions, request.indicesOptions)
            && Objects.equals(query, request.query)
            && Objects.equals(routing, request.routing)
            && Objects.equals(preference, request.preference)
            && allowPartialSearchResults == request.allowPartialSearchResults
            && Objects.equals(clusterAlias, request.clusterAlias);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, query, routing, preference, allowPartialSearchResults, clusterAlias);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
