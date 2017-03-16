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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class StartOpsRecoveryRequest extends StartRecoveryRequest {

    private long startingSeqNo;

    StartOpsRecoveryRequest() {
    }

    /**
     * Construct a request for starting a peer recovery.
     *
     * @param shardId           the shard ID to recover
     * @param sourceNode        the source node to remover from
     * @param targetNode        the target node to recover to
     * @param recoveryId        the recovery ID
     * @param startingSeqNo     the starting sequence number
     */
    public StartOpsRecoveryRequest(final ShardId shardId,
                                final DiscoveryNode sourceNode,
                                final DiscoveryNode targetNode,
                                final long recoveryId,
                                final long startingSeqNo) {
        super(shardId, sourceNode, targetNode, recoveryId);
        this.startingSeqNo = startingSeqNo;
    }

    public long getStartingSeqNo() {
        return startingSeqNo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        startingSeqNo = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(startingSeqNo);
    }
}
