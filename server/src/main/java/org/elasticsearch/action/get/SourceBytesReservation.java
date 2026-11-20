/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.get.GetResult;

/**
 * Charges the {@code _source} of the documents in one multi-get shard response to the request breaker, so that a document the node
 * has no room for fails alone instead of exhausting the heap. The charge is held until the shard response has been serialized, or
 * handed to the caller when the shard is local.
 * <p>
 * Sources are charged in steps of at least {@link #CHARGE_THRESHOLD_BYTES} to limit breaker calls, so a request can leave less than
 * that uncharged. Not thread-safe, since documents are added one at a time and the reservation is closed once the shard request completes.
 */
final class SourceBytesReservation implements Releasable {

    /** The label of the charges, whose metrics category is {@link ChildMemoryCircuitBreaker#CATEGORY_GET}. */
    static final String BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_GET + "[source]";

    static final long CHARGE_THRESHOLD_BYTES = ByteSizeValue.ofMb(1).getBytes();

    private final CircuitBreaker circuitBreaker;
    private long unchargedBytes;
    private long chargedBytes;

    SourceBytesReservation(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Charges the source of a document added to the response.
     *
     * @throws CircuitBreakingException if the breaker has no room. The document isn't charged, so the caller must drop it.
     */
    void add(GetResult result) {
        final BytesReference source = result.internalSourceRef();
        if (source == null) {
            return;
        }
        final long pendingBytes = unchargedBytes + source.length();
        if (pendingBytes < CHARGE_THRESHOLD_BYTES) {
            unchargedBytes = pendingBytes;
        } else {
            circuitBreaker.addEstimateBytesAndMaybeBreak(pendingBytes, BREAKER_LABEL);
            chargedBytes += pendingBytes;
            unchargedBytes = 0;
        }
    }

    @Override
    public void close() {
        if (chargedBytes > 0) {
            circuitBreaker.addWithoutBreaking(-chargedBytes, BREAKER_LABEL);
            chargedBytes = 0;
        }
    }
}
