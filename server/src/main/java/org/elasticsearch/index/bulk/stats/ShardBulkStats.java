/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.SizeBlockingQueue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Internal class that maintains relevant shard bulk statistics / metrics.
 * @see IndexShard
 */
public class ShardBulkStats implements BulkOperationListener {

    private final StatsHolder totalStats = new StatsHolder();
    public final AtomicInteger outstandingRequests = new AtomicInteger();
    public final AtomicLong time = new AtomicLong();

    private final Queue<Integer> freeElement = new ConcurrentLinkedDeque<>(List.of(0, 1, 2, 3));
    private final AtomicLongArray outstandingRequestStartTime = new AtomicLongArray(4);
    private final long[] startTime = new long[4];

    private final LongAdder total = new LongAdder();
    private static final double ALPHA = 0.1;

    private final Map<Long, Long> req = ConcurrentCollections.newConcurrentMap();

    private final Object mutex = new Object();

    private final ShardId shardId;

    public ShardBulkStats(ShardId shardId) {
        this.shardId = shardId;
    }

    public BulkStats stats() {
        return totalStats.stats();
    }

    @Override public int beforeBulk() {
        time.set(System.nanoTime());
        outstandingRequests.incrementAndGet();

        synchronized (mutex) {
            int slot = freeElement.remove();
            startTime[slot] = System.nanoTime();
            //final boolean replaced = outstandingRequestStartTime.compareAndSet(slot, 0, System.nanoTime());
            return slot;
        }
    }

    @Override
    public void failedBulk(int reqId) {
        outstandingRequests.decrementAndGet();
        synchronized (mutex) {
            startTime[reqId] = 0;
            freeElement.add(reqId);
        }
//        long ts;
//        do {
//            ts = outstandingRequestStartTime.get(reqId);
//        } while (outstandingRequestStartTime.compareAndSet(reqId, ts, 0) == false);
    }

    @Override
    public void afterBulk(int reqId, long shardBulkSizeInBytes, long tookInNanos) {
        long ts;
        synchronized (mutex) {
            ts = startTime[reqId];
            startTime[reqId] = 0;
            freeElement.add(reqId);
        }
//        long ts;
//        do {
//            ts = outstandingRequestStartTime.get(reqId);
//        } while (outstandingRequestStartTime.compareAndSet(reqId, ts, 0) == false);
//
//        freeElement.add(reqId);

        if (ts > 0) {
            long total = System.nanoTime() - ts;
            logger.info(
                "---> AB {} / {} {} {}",
                shardId,
                TimeUnit.NANOSECONDS.toMillis(total),
                TimeUnit.NANOSECONDS.toMillis(tookInNanos),
                Math.abs(total - tookInNanos) > 0 ? "xxx" + TimeUnit.NANOSECONDS.toMillis(Math.abs(total - tookInNanos)) : ""
            );
            totalStats.shardBulkMetric.inc(total);
        } else {
            logger.info("---> AB strange {} {}", reqId, shardId);
        }

        // increment total
        totalStats.totalSizeInBytes.inc(shardBulkSizeInBytes);

        totalStats.timeInMillis.addValue(tookInNanos);
        totalStats.sizeInBytes.addValue(shardBulkSizeInBytes);
    }

    private final Logger logger = LogManager.getLogger(ShardBulkStats.class);

    @Override
    public long totalTime() {
        long totalTime = 0;
        for (int i = 0; i < 4; i++) {
            long ts;
            synchronized (mutex) {
                ts = startTime[i];
                if (ts > 0) {
                    startTime[i] = System.nanoTime();
                }
            }
            if (ts > 0) {
                totalTime += (System.nanoTime() - ts);
            }
//            long startTime = outstandingRequestStartTime.get(i);
//            if (startTime > 0) {
//                long ts;
//                do {
//                    ts = outstandingRequestStartTime.get(i);
//                } while (ts > 0 && outstandingRequestStartTime.compareAndSet(i, ts, System.nanoTime()) == false);
//                if (ts > 0) {
//                    totalTime += (System.nanoTime() - ts);
//                }
//            }
        }
        totalStats.shardBulkMetric.inc(totalTime);
        logger.info("---> {} / {}", shardId, TimeUnit.NANOSECONDS.toMillis(totalTime));

        return totalStats.shardBulkMetric.sum();
    }

    static final class StatsHolder {
        final MeanMetric shardBulkMetric = new MeanMetric();
        final CounterMetric totalSizeInBytes = new CounterMetric();
        ExponentiallyWeightedMovingAverage timeInMillis = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);
        ExponentiallyWeightedMovingAverage sizeInBytes = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);

        BulkStats stats() {
            return new BulkStats(
                shardBulkMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(shardBulkMetric.sum()),
                totalSizeInBytes.count(),
                TimeUnit.NANOSECONDS.toMillis((long) timeInMillis.getAverage()),
                (long) sizeInBytes.getAverage()
            );
        }
    }
}
