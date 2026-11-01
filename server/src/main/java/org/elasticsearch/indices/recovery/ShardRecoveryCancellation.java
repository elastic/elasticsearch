/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/// Details of a single shard recovery to be cancelled on a data node.
public record ShardRecoveryCancellation(ShardId shardId, String allocationId, boolean cancelIfStarted) implements Writeable {

    public ShardRecoveryCancellation(StreamInput in) throws IOException {
        this(new ShardId(in), in.readString(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(allocationId);
        out.writeBoolean(cancelIfStarted);
    }
}
