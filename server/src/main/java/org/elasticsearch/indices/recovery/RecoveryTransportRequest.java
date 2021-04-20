/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class RecoveryTransportRequest extends TransportRequest {

    private final long requestSeqNo;

    RecoveryTransportRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            requestSeqNo = in.readLong();
        } else {
            requestSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
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
        if (out.getVersion().onOrAfter(org.elasticsearch.Version.V_7_9_0)) {
            out.writeLong(requestSeqNo);
        }
    }
}
