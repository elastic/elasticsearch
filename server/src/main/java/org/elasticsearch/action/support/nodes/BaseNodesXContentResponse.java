/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public abstract class BaseNodesXContentResponse<TNodeResponse extends BaseNodeResponse> extends BaseNodesResponse<TNodeResponse>
    implements
        ChunkedToXContent {

    protected BaseNodesXContentResponse(ClusterName clusterName, List<TNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    protected BaseNodesXContentResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((b, p) -> {
            b.startObject();
            RestActions.buildNodesHeader(b, p, this);
            return b.field("cluster_name", getClusterName().value());
        }), xContentChunks(params), ChunkedToXContentHelper.endObject());
    }

    protected abstract Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams);

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
