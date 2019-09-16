/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A throttling consumer that forwards input to an outbound consumer, but discarding any that arrive too quickly (but eventually sending the
 * last state after the minimumInterval).
 * The outbound consumer is only called by a single thread at a time. The outbound consumer should be non-blocking.
 * TODO: could be moved to more generic place for reuse?
 * @param <T> type of object passed through.
 */
public class ThrottlingConsumer<T> implements Consumer<T> {
    private final ThreadPool threadPool;
    private final BiConsumer<T, Runnable> outbound;
    private final TimeValue minimumInterval;
    private final Object lock = new Object();
    private final LongSupplier nanoTimeSource;

    // state protected by lock.
    private long lastWriteTimeNanos;
    private T value;
    private Scheduler.ScheduledCancellable scheduledWrite;
    private boolean outboundActive;
    private boolean closed;
    private Runnable onClosed;

    public ThrottlingConsumer(BiConsumer<T, Runnable> outbound, TimeValue minimumInterval,
                              LongSupplier nanoTimeSource, ThreadPool threadPool) {
        this.outbound = outbound;
        this.minimumInterval = minimumInterval;
        this.threadPool = threadPool;
        this.nanoTimeSource = nanoTimeSource;
        this.lastWriteTimeNanos = nanoTimeSource.getAsLong();
    }

    @Override
    public void accept(T newValue) {
        long now = nanoTimeSource.getAsLong();
        synchronized (lock) {
            if (closed) {
                return;
            }
            this.value = newValue;
            if (scheduledWrite == null) {
                // schedule is non-blocking
                scheduledWrite = threadPool.schedule(this::onScheduleTimeout, getDelay(now), ThreadPool.Names.SAME);
            }
        }
    }

    private TimeValue getDelay(long now) {
        long nanos = lastWriteTimeNanos + minimumInterval.nanos() - now;
        return nanos < 0 ? TimeValue.ZERO : TimeValue.timeValueNanos(nanos);
    }

    private void onScheduleTimeout() {
        T value;
        long now = nanoTimeSource.getAsLong();
        synchronized (lock) {
            if (closed) {
                return;
            }
            value = this.value;
            lastWriteTimeNanos = now;
            outboundActive = true;
        }

        outbound.accept(value, () -> {
            synchronized (this) {
                outboundActive = false;
                if (closed == false) {
                    if (value != this.value) {
                        scheduledWrite = threadPool.schedule(this::onScheduleTimeout, minimumInterval,
                            ThreadPool.Names.SAME);
                    } else {
                        scheduledWrite = null;
                    }
                }
            }

            // safe since onScheduleTimeout is only called single threaded
            if (onClosed != null) {
                onClosed.run();
            }
        });
    }

    /**
     * Async close this. Any state submitted since last outbound call will be discarded (as well as any new inbound accept calls).
     * @param onClosed called when closed, which guarantees no more calls on outbound consumer. Must be non-blocking.
     */
    public void close(Runnable onClosed) {
        synchronized (lock) {
            assert closed == false : "multiple closes not supported";
            closed = true;
            if (scheduledWrite != null) {
                if (outboundActive) {
                    this.onClosed = onClosed;
                } else {
                    scheduledWrite.cancel();
                }
                scheduledWrite = null;
            }
        }
        if (this.onClosed == null) {
            onClosed.run();
        }
    }
}
