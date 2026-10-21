/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/// Encapsulates information about the shard recoveries which have failed and are being retried locally on a data node.
public record LocalRecoveryRetriesHealthInfo(Map<ShardId, Integer> localRetries) implements Writeable {

    public static final LocalRecoveryRetriesHealthInfo EMPTY = new LocalRecoveryRetriesHealthInfo(Map.of());

    public LocalRecoveryRetriesHealthInfo {
        localRetries = Map.copyOf(localRetries);
    }

    public LocalRecoveryRetriesHealthInfo(StreamInput in) throws IOException {
        this(in.readMap(ShardId::new, StreamInput::readVInt));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(localRetries, StreamOutput::writeWriteable, StreamOutput::writeVInt);
    }
}
