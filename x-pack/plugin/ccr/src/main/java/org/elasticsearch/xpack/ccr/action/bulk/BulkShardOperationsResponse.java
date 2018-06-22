/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class BulkShardOperationsResponse extends ReplicationResponse implements WriteResponse {

    private long localCheckpoint;

    BulkShardOperationsResponse(long localCheckPoint) {
        this.localCheckpoint = localCheckPoint;
    }

    BulkShardOperationsResponse() {
    }

    public long getLocalCheckpoint() {
        return localCheckpoint;
    }

    @Override
    public void setForcedRefresh(final boolean forcedRefresh) {
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        localCheckpoint = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(localCheckpoint);
    }
}
