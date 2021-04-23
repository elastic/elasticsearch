/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class RecoveryTransportRequest extends TransportRequest {

    private final long requestSeqNo;

    RecoveryTransportRequest(StreamInput in) throws IOException {
        super(in);
        requestSeqNo = in.readLong();
    }

    RecoveryTransportRequest(long requestSeqNo) {
        this.requestSeqNo = requestSeqNo;
    }

    public long requestSeqNo() {
        return requestSeqNo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(requestSeqNo);
    }
}
