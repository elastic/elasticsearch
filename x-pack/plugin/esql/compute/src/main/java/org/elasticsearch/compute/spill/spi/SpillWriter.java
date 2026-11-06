/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;

/**
 * Sequential append of one object. States: open, finishing, finished; open, aborting, aborted; and failed, which a failure
 * of any operation leads to and which every later operation reports. Append and finish are serialized by the caller,
 * one outstanding at a time; a call out of sequence completes its listener with {@link IllegalStateException} and poisons
 * the writer. No method throws for I/O, capacity or memory.
 */
public interface SpillWriter extends ChunkSink {

    /**
     * Appends the chunk under the borrow rule of {@link ChunkSink}; the listener completes when the backend has taken the
     * bytes and relinquished its reference, with {@link SpillLimitExceededException} when the cap would be exceeded.
     */
    @Override
    void append(ReleasableBytesReference chunk, ActionListener<Void> listener);

    /**
     * Publishes the object, after every append has completed. Exactly one object is published per writer; the writer
     * is finished afterwards and its abort is a no-op.
     */
    void finish(ActionListener<SpillObject> listener);

    /**
     * Discards what was appended, returning its bytes to the quota. Idempotent; a no-op after finish.
     */
    void abort(ActionListener<Void> listener);
}
