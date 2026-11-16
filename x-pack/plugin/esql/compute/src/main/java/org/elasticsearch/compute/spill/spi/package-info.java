/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * The storage SPI of ES|QL spilling: what a backend module implements so that the engine can put bytes aside and read
 * them back. The engine owns every format; a backend moves bytes it does not understand.
 * <p>
 * Every operation is asynchronous and completes a listener, never throws for I/O, capacity or memory, and completes that
 * listener exactly once: inline, on the backend's own thread, after an executor rejection, after cancellation, or after
 * a failure. A state transition happens before its callback, so a callback may issue the next operation at once. The
 * invariants, enforced by the conformance suite in {@code org.elasticsearch.compute.test.spill}:
 * <ul>
 *     <li>One outstanding append per writer and one outstanding read per reader. {@code finish} is ordered after every
 *     append and publishes exactly one immutable object. A call that breaks the sequence completes its listener with an
 *     {@link java.lang.IllegalStateException} and poisons the writer.</li>
 *     <li>An accepted append takes its own reference to the bytes and relinquishes it before the terminal callback; the
 *     caller keeps independent ownership throughout.</li>
 *     <li>Short reads are valid. End of range is distinct from empty data. Range arithmetic and the maximum read size are
 *     validated before anything is allocated.</li>
 *     <li>A failure delivered to a writer poisons it; a failure delivered to a reader poisons that reader. Abort, close,
 *     delete and shutdown are idempotent. A read issued after a delete fails with {@link java.lang.IllegalStateException};
 *     a read outstanding at the delete completes either way, as an open channel would; the object's bytes return to
 *     the quota when the delete completes.</li>
 *     <li>Cancellation requests cessation; a scope's close completes only when no worker touches borrowed memory or a
 *     resource any more; cancellation never waits for a driver to finish before starting.</li>
 *     <li>Inline completion is permitted and never expected: a backend completes inline only when the operation was
 *     satisfied from a buffer or a queue.</li>
 * </ul>
 */
package org.elasticsearch.compute.spill.spi;
