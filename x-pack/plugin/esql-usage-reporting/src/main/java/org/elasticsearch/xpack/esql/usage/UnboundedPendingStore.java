/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link PendingStore} backed by a plain list, for tests and for the reporter before the bounded store exists. It is the default
 * of {@link UsageComponents#newPendingStore} only until the store change replaces it, and that change must merge before, or
 * together with, the reporter: a reporter that fills this store while the publisher keeps failing would grow it without bound.
 */
public final class UnboundedPendingStore implements PendingStore {

    private static final Logger logger = LogManager.getLogger(UnboundedPendingStore.class);

    private final List<UsageRecord> records = new ArrayList<>();

    @Override
    public synchronized void add(List<UsageRecord> toAdd) {
        records.addAll(toAdd);
    }

    @Override
    public synchronized List<UsageRecord> next(int max) {
        return List.copyOf(records.subList(0, Math.min(max, records.size())));
    }

    @Override
    public synchronized void acknowledge(Set<String> ids) {
        records.removeIf(r -> ids.contains(r.id()));
    }

    @Override
    public synchronized void drop(Set<String> ids, String reason) {
        for (UsageRecord record : records) {
            if (ids.contains(record.id())) {
                logger.error(
                    "dropping usage record [{}] of type [{}] with quantity [{}] ms: {}",
                    record.id(),
                    record.usageType(),
                    record.quantityMillis(),
                    reason
                );
            }
        }
        records.removeIf(r -> ids.contains(r.id()));
    }

    @Override
    public synchronized int size() {
        return records.size();
    }

    @Override
    public synchronized Optional<Instant> oldestUsageTimestamp() {
        return records.stream().map(UsageRecord::usageTimestamp).min(Instant::compareTo);
    }
}
