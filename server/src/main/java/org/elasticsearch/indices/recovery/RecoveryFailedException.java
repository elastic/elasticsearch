/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class RecoveryFailedException extends ElasticsearchException {

    public RecoveryFailedException(StartRecoveryRequest request, Throwable cause) {
        this(request, null, cause);
    }

    public RecoveryFailedException(StartRecoveryRequest request, @Nullable String extraInfo, Throwable cause) {
        this(request.shardId(), request.sourceNode(), request.targetNode(), extraInfo, cause);
    }

    public RecoveryFailedException(RecoveryState state, @Nullable String extraInfo, Throwable cause) {
        this(state.getShardId(), state.getSourceNode(), state.getTargetNode(), extraInfo, cause);
    }

    public RecoveryFailedException(
        ShardId shardId,
        DiscoveryNode sourceNode,
        DiscoveryNode targetNode,
        @Nullable String extraInfo,
        Throwable cause
    ) {
        super(
            shardId
                + ": Recovery failed "
                + (sourceNode != null ? "from " + sourceNode + " into " : "on ")
                + targetNode
                + (extraInfo == null ? "" : " (" + extraInfo + ")"),
            cause
        );
    }

    public RecoveryFailedException(StreamInput in) throws IOException {
        super(in);
    }
}
