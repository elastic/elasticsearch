/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;

/**
 *
 */
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

    public RecoveryFailedException(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, Throwable cause) {
        this(shardId, sourceNode, targetNode, null, cause);
    }

    public RecoveryFailedException(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, @Nullable String extraInfo, Throwable cause) {
        super(shardId + ": Recovery failed from " + sourceNode + " into " + targetNode + (extraInfo == null ? "" : " (" + extraInfo + ")"), cause);
    }
}
