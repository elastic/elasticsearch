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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class StartRecoveryRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;

    StartRecoveryRequest() {
    }

    /**
     * Construct a request for starting a peer recovery.
     *
     * @param shardId    the shard ID to recover
     * @param sourceNode the source node to remover from
     * @param targetNode the target node to recover to
     * @param recoveryId the recovery ID
     */
    public StartRecoveryRequest(final ShardId shardId,
                                final DiscoveryNode sourceNode,
                                final DiscoveryNode targetNode,
                                final long recoveryId) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public DiscoveryNode sourceNode() {
        return sourceNode;
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
    }

    /**
     * checks the source's shard routing to see if recovery can start and throws the appropriate
     * exception if not.
     *
     * @param sourceRouting source routing entry
     * @param logger        logger to log any debug info on
     */
    public void validateSourceRouting(ShardRouting sourceRouting, Logger logger) {}

    public void validateTargetRouting(ShardRouting targetRouting, Logger logger) {
        if (targetRouting == null) {
            logger.debug(
                "delaying recovery of {} as it is not listed as assigned to target node {}",
                shardId(), targetNode());
            throw new DelayRecoveryException(
                "source node does not have the shard listed in its state as allocated on the node");
        }
        if (!targetRouting.initializing()) {
            logger.debug("delaying recovery of {} as it is not listed as initializing on the " +
                    "target node {}. known shards state is [{}]",
                shardId(), targetNode(), targetRouting.state());
            throw new DelayRecoveryException("source node has the state of the target shard to " +
                "be [" + targetRouting.state() + "], expecting to be [initializing]");
        }
    }
}
