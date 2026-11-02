/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Asks one data node to free fetch contexts that the coordinator of their query no longer reads, all of them in one request.
 * The free action of {@code _search} takes one context per request, so a query over many shards would send one request per
 * shard.
 * <p>
 * It carries the index expressions of the query, so it is authorized like the fetch request, and the node frees only the
 * contexts of the indices the request may read.
 */
public final class FetchContextFreeRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final List<ShardSearchContextId> contextIds;

    /**
     * @param originalIndices the index expressions of the execution that opened the contexts
     */
    FetchContextFreeRequest(OriginalIndices originalIndices, List<ShardSearchContextId> contextIds) {
        if (originalIndices.indices().length == 0) {
            // a request without index expressions is authorized for every index
            throw new IllegalArgumentException("a free needs the index expressions of its query");
        }
        if (contextIds.isEmpty()) {
            throw new IllegalArgumentException("a free needs at least one context");
        }
        this.indices = originalIndices.indices();
        this.indicesOptions = originalIndices.indicesOptions();
        this.contextIds = List.copyOf(contextIds);
    }

    FetchContextFreeRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.contextIds = in.readCollectionAsImmutableList(ShardSearchContextId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(FetchRequest.ESQL_FETCH) == false) {
            // only nodes that kept contexts for the fetch phase get a free
            throw new IllegalStateException("can't send a fetch context free to a node on [" + out.getTransportVersion() + "]");
        }
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeCollection(contextIds);
    }

    public List<ShardSearchContextId> contextIds() {
        return contextIds;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Keeps the contexts even when authorization leaves no index: the node then frees none of them, because the index of
     * no context is in the access control of the request.
     */
    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public String getDescription() {
        return "free fetch contexts=" + contextIds.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchContextFreeRequest that = (FetchContextFreeRequest) o;
        return Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && contextIds.equals(that.contextIds)
            && getParentTask().equals(that.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, contextIds, getParentTask());
    }
}
