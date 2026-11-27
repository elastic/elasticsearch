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
 * not been applied on this copy yet (recovery and CCR deliver operations concurrently, so a delta can race ahead of its base). How the
 * failure is handled depends on the path: a CCR follower retries the operation (the fetch re-delivers the base, in order, first, via
 * {@code ShardFollowNodeTask#shouldRetry}), while on normal peer replication it is not individually retried - the recovering copy is
 * failed and re-recovers, bringing the base with it. Either way the base is present on the next attempt. Not a client error. The real fix,
 * a replica-side wait-for-base, is tracked as a follow-up (see issue #166843).
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
