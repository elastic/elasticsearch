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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link PendingStore} that holds at most {@code maxSize} records, oldest first. When the Usage API stays unreachable the
 * oldest records are dropped to make room, because memory must stay bounded; dropping can only under-count, never over-count.
 * Every record that leaves the store without being accepted leaves an ERROR log line with its id, usage type and quantity, and
 * is counted in {@link #droppedCount()}, so that no loss is silent.
 * <p>
 * Records are kept in memory only. A crash loses what is pending (also an under-count); a disk spool was left out on purpose
 * to keep the audited path simple.
 * <p>
 * A record whose id is already pending is not added again: a retry carries the same id and must never be counted twice. If the
 * record with that id differs from the pending one, which would be a bug in the producer of the ids, a WARN is logged and the pending
 * copy is kept.
 */
public final class BoundedPendingStore implements PendingStore {

    private static final Logger logger = LogManager.getLogger(BoundedPendingStore.class);

    private final int maxSize;
    /** Built once so that the log call does not concatenate strings per lost record. */
    private final String fullReason;
    /** Insertion ordered by id; guarded by {@code this}. */
    private final LinkedHashMap<String, UsageRecord> records = new LinkedHashMap<>();
    private final AtomicLong dropped = new AtomicLong();

    public BoundedPendingStore(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1 but was [" + maxSize + "]");
        }
        this.maxSize = maxSize;
        this.fullReason = "the pending store is full, maximum [" + maxSize + "]";
    }

    @Override
    public synchronized void add(List<UsageRecord> toAdd) {
        for (UsageRecord record : toAdd) {
            UsageRecord existing = records.putIfAbsent(record.id(), record);
            if (existing != null && existing.equals(record) == false) {
                logger.warn(
                    "ignoring usage record [{}] of type [{}] with quantity [{}] ms: a different record with the same id is pending "
                        + "(type [{}], quantity [{}] ms); ids must identify one record",
                    record.id(),
                    record.usageType(),
                    record.quantityMillis(),
                    existing.usageType(),
                    existing.quantityMillis()
                );
            }
        }
        Iterator<UsageRecord> oldest = records.values().iterator();
        while (records.size() > maxSize) {
            UsageRecord record = oldest.next();
            oldest.remove();
            logLoss(record, fullReason);
        }
    }

    @Override
    public synchronized List<UsageRecord> next(int max) {
        List<UsageRecord> result = new ArrayList<>(Math.min(max, records.size()));
        for (UsageRecord record : records.values()) {
            if (result.size() >= max) {
                break;
            }
            result.add(record);
        }
        return result;
    }

    @Override
    public synchronized void acknowledge(Set<String> ids) {
        records.keySet().removeAll(ids);
    }

    @Override
    public synchronized void drop(Set<String> ids, String reason) {
        for (String id : ids) {
            UsageRecord record = records.remove(id);
            if (record != null) {
                logLoss(record, reason);
            }
        }
    }

    @Override
    public synchronized int size() {
        return records.size();
    }

    @Override
    public synchronized Optional<Instant> oldestUsageTimestamp() {
        return records.values().stream().map(UsageRecord::usageTimestamp).min(Instant::compareTo);
    }

    /** How many records were lost, by the bound or by {@link #drop}, since the store was created; for the metrics. */
    long droppedCount() {
        return dropped.get();
    }

    private void logLoss(UsageRecord record, String reason) {
        dropped.incrementAndGet();
        logger.error(
            "dropping usage record [{}] of type [{}] with quantity [{}] ms: {}",
            record.id(),
            record.usageType(),
            record.quantityMillis(),
            reason
        );
    }
}
