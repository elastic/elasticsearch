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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class is a container for the various decisions took, if any, for where to
 * allocate or move a single shard.
 */
public final class ShardAllocationDecision implements ToXContent, Writeable {
    private final AllocateUnassignedDecision allocateDecision;
    private final MoveDecision moveDecision;

    public ShardAllocationDecision(AllocateUnassignedDecision allocateDecision,
                                   MoveDecision moveDecision) {
        this.allocateDecision = allocateDecision;
        this.moveDecision = moveDecision;
    }

    public ShardAllocationDecision(StreamInput in) throws IOException {
        allocateDecision = in.readOptionalWriteable(AllocateUnassignedDecision::new);
        moveDecision = in.readOptionalWriteable(MoveDecision::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(allocateDecision);
        out.writeOptionalWriteable(moveDecision);
    }

    /**
     * Gets the unassigned allocation decision for the shard.  If the shard was not in the unassigned state,
     * the instance of {@link AllocateUnassignedDecision} that is returned will have {@link AllocateUnassignedDecision#isDecisionTaken()}
     * return {@code false}.
     */
    public AllocateUnassignedDecision getAllocateDecision() {
        return allocateDecision;
    }

    /**
     * Gets the move decision for the shard.  If the shard was in the unassigned state,
     * the instance of {@link MoveDecision} that is returned will have {@link MoveDecision#isDecisionTaken()}
     * return {@code false}.
     */
    public MoveDecision getMoveDecision() {
        return moveDecision;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (allocateDecision.isDecisionTaken()) {
            allocateDecision.toXContent(builder, params);
        }
        if (moveDecision.isDecisionTaken()) {
            moveDecision.toXContent(builder, params);
        }
        return builder;
    }

}
