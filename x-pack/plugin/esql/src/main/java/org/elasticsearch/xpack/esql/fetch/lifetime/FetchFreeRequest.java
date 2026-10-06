/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

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

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;

/**
 * Frees fetch contexts on one data node. It carries the index expressions of the query, so it is authorized like the
 * query's own requests to that node, with the same wildcards and aliases.
 */
public final class FetchFreeRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable, FetchContextRequest {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private List<ShardSearchContextId> contextIds;

    public FetchFreeRequest(OriginalIndices originalIndices, List<ShardSearchContextId> contextIds) {
        this.indices = originalIndices.indices();
        this.indicesOptions = originalIndices.indicesOptions();
        this.contextIds = List.copyOf(contextIds);
    }

    public FetchFreeRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.contextIds = in.readCollectionAsImmutableList(ShardSearchContextId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
     * When authorization leaves no index, the request frees nothing, and the reaper frees the contexts after their
     * keep-alive.
     */
    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        if (Arrays.equals(NO_INDICES_OR_ALIASES_ARRAY, indices) || Arrays.asList(indices).contains(NO_INDEX_PLACEHOLDER)) {
            this.contextIds = List.of();
        }
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
        return "free " + contextIds.size() + " fetch contexts";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchFreeRequest that = (FetchFreeRequest) o;
        return Arrays.equals(indices, that.indices) && indicesOptions.equals(that.indicesOptions) && contextIds.equals(that.contextIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, contextIds);
    }
}
