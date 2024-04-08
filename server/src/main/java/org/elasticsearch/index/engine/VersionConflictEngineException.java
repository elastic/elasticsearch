/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class VersionConflictEngineException extends EngineException {
    public VersionConflictEngineException(
        ShardId shardId,
        String id,
        long compareAndWriteSeqNo,
        long compareAndWriteTerm,
        long currentSeqNo,
        long currentTerm
    ) {
        this(
            shardId,
            "[" + id + "]",
            "required seqNo ["
                + compareAndWriteSeqNo
                + "], primary term ["
                + compareAndWriteTerm
                + "]."
                + (currentSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                    ? " but no document was found"
                    : " current document has seqNo [" + currentSeqNo + "] and primary term [" + currentTerm + "]")
        );
    }

    public VersionConflictEngineException(ShardId shardId, String documentDescription, String explanation) {
        this(shardId, "{}: version conflict, {}", null, documentDescription, explanation);
    }

    public VersionConflictEngineException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(shardId, msg, cause, params);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public VersionConflictEngineException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for updates; stack traces are expensive to compute and not very useful for VCEEs, so don't fill it in.
        return this;
    }
}
