/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Allocates all unallocated primary shards. This will cause data loss on those
 * shards so only use this in an emergency. Prefer
 * {@code AllocateAllocationCommand} with <tt>allowPrimary</tt> if you have a
 * specific master to assign as this will just do them all.
 */
public class AllocateAllPrimariesAllocationCommand implements AllocationCommand {
    public static final String NAME = "allocate_all_primaries";

    public static class Factory implements AllocationCommand.Factory<AllocateAllPrimariesAllocationCommand> {

        @Override
        public AllocateAllPrimariesAllocationCommand readFrom(StreamInput in) throws IOException {
            return new AllocateAllPrimariesAllocationCommand();
        }

        @Override
        public void writeTo(AllocateAllPrimariesAllocationCommand command, StreamOutput out) throws IOException {
        }

        @Override
        public AllocateAllPrimariesAllocationCommand fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            if ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                throw new ElasticSearchParseException("[allocate_all_primaries] command does not support complex json tokens [" + token
                        + "]");
            }
            return new AllocateAllPrimariesAllocationCommand();
        }

        @Override
        public void toXContent(AllocateAllPrimariesAllocationCommand command, XContentBuilder builder, ToXContent.Params params)
                throws IOException {
            builder.startObject();
            builder.endObject();
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void execute(RoutingAllocation allocation) throws ElasticSearchException {
        boolean found = false;
        for (MutableShardRouting routing : allocation.routingNodes().unassigned()) {
            if (routing.primary()) {
                found = true;
                // Just clear the post allocation flag to the shard so it'll assign itself.
                allocation.routingNodes().addClearPostAllocationFlag(routing.shardId());
            }
        }

        if (!found) {
            throw new ElasticSearchIllegalArgumentException("[allocate_all_primaries] no unassigned primaries");
        }
    }
}
