/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.broadcast.unpromotable;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class UnpromotableShardStats extends ActionResponse {
    private final int successful;
    private final int failed;

    public UnpromotableShardStats(int successful, int failed) {
        this.successful = successful;
        this.failed = failed;
    }

    public UnpromotableShardStats(StreamInput in) throws IOException {
        super(in);
        this.successful = in.readVInt();
        this.failed = in.readVInt();
    }

    public int getSuccessful() {
        return successful;
    }

    public int getFailed() {
        return failed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(successful);
        out.writeVInt(failed);
    }
}
