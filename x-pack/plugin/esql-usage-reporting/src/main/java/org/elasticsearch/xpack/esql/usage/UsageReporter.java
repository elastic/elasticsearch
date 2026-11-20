/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.io.Closeable;
import java.time.Instant;

/**
 * Runs the periodic cycle on its own thread: aligned to wall-clock multiples of the period, {@link #tick} drains the
 * {@link UsageAccumulator}, builds records, adds them to the {@link PendingStore} and publishes batches until the store is empty
 * or a failure says to stop. A publish result that accepts nothing and reports no failure also stops the cycle, so that the same
 * batch is never requested in a loop. Single flight: a cycle that is still running is not started again. {@link #close} makes a final
 * flush attempt and may lose usage, which can only under-count.
 */
public interface UsageReporter extends Closeable {

    void start();

    /** One cycle. Public so that tests drive it with a fake clock. */
    void tick(Instant now);

    @Override
    void close();
}
