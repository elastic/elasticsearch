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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Asks one data node to keep fetch contexts open for another keep-alive, because the coordinator of their query still holds
 * them. Without it, a query that waits for a slow node could lose the readers of the nodes that finished first.
 * <p>
 * It carries the index expressions of the query, so it is authorized like the fetch request. The node checks the shard of
 * every context against the access control of the request.
 */
public final class FetchContextKeepAliveRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final List<ShardSearchContextId> contextIds;
    private final TimeValue keepAlive;

    /**
     * @param originalIndices the index expressions of the execution that opened the contexts
     * @param keepAlive       how long each context stays open after this request, at least
     */
    FetchContextKeepAliveRequest(OriginalIndices originalIndices, List<ShardSearchContextId> contextIds, TimeValue keepAlive) {
        if (originalIndices.indices().length == 0) {
            // a request without index expressions is authorized for every index
            throw new IllegalArgumentException("a keep-alive needs the index expressions of its query");
        }
        if (contextIds.isEmpty()) {
            throw new IllegalArgumentException("a keep-alive needs at least one context");
        }
        if (keepAlive.millis() <= 0) {
            // reader contexts keep their keep-alive in milliseconds
            throw new IllegalArgumentException("the keep-alive must be at least a millisecond but was [" + keepAlive + "]");
        }
        this.indices = originalIndices.indices();
        this.indicesOptions = originalIndices.indicesOptions();
        this.contextIds = List.copyOf(contextIds);
        this.keepAlive = keepAlive;
    }

    FetchContextKeepAliveRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.contextIds = in.readCollectionAsImmutableList(ShardSearchContextId::new);
        this.keepAlive = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(FetchRequest.ESQL_FETCH) == false) {
            // only nodes that kept contexts for the fetch phase get a keep-alive
            throw new IllegalStateException("can't send a fetch context keep-alive to a node on [" + out.getTransportVersion() + "]");
        }
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeCollection(contextIds);
        out.writeTimeValue(keepAlive);
    }

    public List<ShardSearchContextId> contextIds() {
        return contextIds;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Keeps the contexts even when authorization leaves no index: the node then rejects the request, because the index of
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
        return "keep alive fetch contexts=" + contextIds.size() + ", keep_alive=" + keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchContextKeepAliveRequest that = (FetchContextKeepAliveRequest) o;
        return Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && contextIds.equals(that.contextIds)
            && keepAlive.equals(that.keepAlive)
            && getParentTask().equals(that.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, contextIds, keepAlive, getParentTask());
    }
}
