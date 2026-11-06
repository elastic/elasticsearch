/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.action.ActionListener;

/**
 * Sequential within its range, one read outstanding at a time; a backend may read ahead on its own. A read that fails
 * poisons the reader: later reads fail with {@link IllegalStateException}, and the caller opens a new range to retry.
 * The one exception is a {@link org.elasticsearch.common.breaker.CircuitBreakingException} on the buffer the read
 * would return, which leaves the reader where it was, so the caller may free memory and read again.
 */
public interface SpillReader {

    /**
     * The next bytes, at most {@code maxBytes} of them and possibly fewer, or the end of the range. The data belongs to
     * the caller, who closes it. {@code maxBytes} is validated before anything is allocated.
     */
    void read(int maxBytes, ActionListener<SpillRead> listener);

    /**
     * Releases the reader; idempotent.
     */
    void close(ActionListener<Void> listener);
}
