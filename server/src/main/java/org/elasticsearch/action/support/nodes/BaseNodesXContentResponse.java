/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.List;

public abstract class BaseNodesXContentResponse<TNodeResponse extends BaseNodeResponse> extends BaseNodesResponse<TNodeResponse>
    implements
        ChunkedToXContentObject {

    protected BaseNodesXContentResponse(ClusterName clusterName, List<TNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public final Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params)
            .object(
                ob -> ob.append((b, p) -> RestActions.buildNodesHeader(b, p, this))
                    .field("cluster_name", getClusterName().value())
                    .append(xContentChunks(params))
            );
    }

    protected abstract Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams);
}
