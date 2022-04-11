/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class RecoveryEngineException extends EngineException {

    private final int phase;

    public RecoveryEngineException(ShardId shardId, int phase, String msg, Throwable cause) {
        super(shardId, "Phase[" + phase + "] " + msg, cause);
        this.phase = phase;
    }

    public RecoveryEngineException(StreamInput in) throws IOException {
        super(in);
        phase = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(phase);
    }

    public int phase() {
        return phase;
    }
}
