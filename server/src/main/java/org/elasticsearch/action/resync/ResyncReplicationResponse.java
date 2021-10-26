/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public final class ResyncReplicationResponse extends ReplicationResponse implements WriteResponse {

    public ResyncReplicationResponse() {}

    public ResyncReplicationResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        // ignore
    }
}
