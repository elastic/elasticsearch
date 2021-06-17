/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;

public final class SearchContextIdForNode implements Writeable {
    private final String node;
    private final ShardSearchContextId searchContextId;
    private final String clusterAlias;

    SearchContextIdForNode(@Nullable String clusterAlias, String node, ShardSearchContextId searchContextId) {
        this.node = node;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
    }

    SearchContextIdForNode(StreamInput in) throws IOException {
        this.node = in.readString();
        this.clusterAlias = in.readOptionalString();
        this.searchContextId = new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(node);
        out.writeOptionalString(clusterAlias);
        searchContextId.writeTo(out);
    }

    public String getNode() {
        return node;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    public ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    @Override
    public String toString() {
        return "SearchContextIdForNode{" +
            "node='" + node + '\'' +
            ", seachContextId=" + searchContextId +
            ", clusterAlias='" + clusterAlias + '\'' +
            '}';
    }
}
