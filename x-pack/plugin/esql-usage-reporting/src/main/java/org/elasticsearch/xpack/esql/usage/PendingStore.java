/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The records that were built but not yet accepted by the Usage API, kept so that they can be sent again with the same ids.
 * Bounded and ordered oldest first; used only by the reporter thread, but implementations must be thread-safe for the metrics
 * that read {@link #size()}. Every loss, whether by the bound or by {@link #drop}, is logged. There is no persistence on purpose:
 * a crash loses what is pending, which can only under-count.
 */
public interface PendingStore {

    /** Adds records, dropping the oldest ones, and logging the loss, if the bound is exceeded. */
    void add(List<UsageRecord> records);

    /** Up to {@code max} records, oldest first, without removing them. */
    List<UsageRecord> next(int max);

    /** The records were accepted by the Usage API: removes them. */
    void acknowledge(Set<String> ids);

    /** The records were rejected for good: removes them and logs the loss with {@code reason}. */
    void drop(Set<String> ids, String reason);

    int size();

    Optional<Instant> oldestUsageTimestamp();
}
