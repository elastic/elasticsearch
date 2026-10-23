/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown on a non-primary copy when an in-place doc-values update cannot be applied because the base version it was computed against has
 * not been applied on this copy yet (recovery and CCR deliver operations concurrently, so a delta can race ahead of its base). Failing
 * the operation causes it to be retried once the base arrives in order. Transient and retryable, not a client error.
 */
public class DocValuesUpdateBaseNotPresentException extends EngineException {

    public static final TransportVersion DOC_VALUES_UPDATE_BASE_NOT_PRESENT_EXCEPTION_VERSION = TransportVersion.fromName(
        "doc_values_update"
    );

    public DocValuesUpdateBaseNotPresentException(ShardId shardId, String id, long seqNo, long baseSeqNo) {
        super(
            shardId,
            "in-place doc-values update for document ["
                + id
                + "] seq_no ["
                + seqNo
                + "] cannot be applied because its base version seq_no ["
                + baseSeqNo
                + "] has not been applied on this shard yet"
        );
    }

    public DocValuesUpdateBaseNotPresentException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        // A transient ordering condition on a non-primary copy; surfaced as a server-side, retryable error rather than a client error.
        return RestStatus.TOO_MANY_REQUESTS;
    }
}
