/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/// Encapsulates information about the shard recoveries which have failed and are being retried locally on a data node.
///
/// The map is keyed by [RecoveryKey], which includes the allocation ID as well as the shard ID, so that a count recorded for one allocation
/// of a shard to a node is not applied to a later allocation of the same shard to the same node (e.g. if the master has since cancelled the
/// earlier one) while the cached instance of this object is out of date.
public record LocalRecoveryRetriesHealthInfo(Map<RecoveryKey, Integer> localRetries) implements Writeable {

    /// Identifies a particular recovery of a shard copy on a node, by the shard and the ID of its allocation (see
    /// [ShardRouting#allocationId()]).
    public record RecoveryKey(ShardId shardId, String allocationId) implements Writeable {

        public RecoveryKey(StreamInput in) throws IOException {
            this(new ShardId(in), in.readString());
        }

        /// Returns the key for the recovery of the shard copy represented by the given routing entry, which must be assigned to a node.
        public static RecoveryKey of(ShardRouting routing) {
            assert routing.allocationId() != null : "no allocation ID for unassigned shard: " + routing;
            return new RecoveryKey(routing.shardId(), routing.allocationId().getId());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeString(allocationId);
        }
    }

    public static final LocalRecoveryRetriesHealthInfo EMPTY = new LocalRecoveryRetriesHealthInfo(Map.of());

    public LocalRecoveryRetriesHealthInfo {
        localRetries = Map.copyOf(localRetries);
    }

    public LocalRecoveryRetriesHealthInfo(StreamInput in) throws IOException {
        this(in.readMap(RecoveryKey::new, StreamInput::readVInt));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(localRetries, StreamOutput::writeWriteable, StreamOutput::writeVInt);
    }
}
