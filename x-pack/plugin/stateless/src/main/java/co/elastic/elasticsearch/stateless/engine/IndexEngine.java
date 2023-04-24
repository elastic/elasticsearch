/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * {@link Engine} implementation for index shards
 */
public class IndexEngine extends InternalEngine {

    public static final Setting<TimeValue> INDEX_FLUSH_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.flush_interval",
        new TimeValue(5, TimeUnit.SECONDS),
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private final TranslogReplicator translogReplicator;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final AtomicLong lastFlushNanos;
    private volatile TimeValue indexFlushInterval;
    private volatile Scheduler.ScheduledCancellable cancellableFlushTask;
    private final ReleasableLock flushLock = new ReleasableLock(new ReentrantLock());

    public IndexEngine(EngineConfig engineConfig, TranslogReplicator translogReplicator) {
        super(engineConfig);
        assert engineConfig.isPromotableToPrimary();
        this.translogReplicator = translogReplicator;
        this.relativeTimeInNanosSupplier = config().getRelativeTimeInNanosSupplier();
        this.lastFlushNanos = new AtomicLong(relativeTimeInNanosSupplier.getAsLong());
        this.indexFlushInterval = INDEX_FLUSH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
        cancellableFlushTask = scheduleFlushTask();
    }

    @Override
    public void onSettingsChanged() {
        super.onSettingsChanged();
        this.indexFlushInterval = INDEX_FLUSH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
        cancellableFlushTask.cancel();
        cancellableFlushTask = scheduleFlushTask();
    }

    private Scheduler.ScheduledCancellable scheduleFlushTask() {
        return engineConfig.getThreadPool().schedule(this::scheduleFlush, indexFlushInterval, ThreadPool.Names.FLUSH);
    }

    private void scheduleFlush() {
        if (isClosed.get()) {
            return;
        }

        TimeValue nextFlushDelay = indexFlushInterval;

        try {
            long sinceLastFlushNanos = relativeTimeInNanosSupplier.getAsLong() - lastFlushNanos.get();
            if (sinceLastFlushNanos < indexFlushInterval.nanos()) {
                // Try to maintain flushes happening within the indexFlushInterval in case of an unscheduled flush
                nextFlushDelay = TimeValue.timeValueNanos(indexFlushInterval.nanos() - sinceLastFlushNanos);
            } else {
                try (ReleasableLock releasableLock = flushLock.tryAcquire()) {
                    if (releasableLock != null) {
                        performScheduledFlush();
                    }
                }
            }
        } catch (AlreadyClosedException e) {
            // Ignore already closed exceptions as this is a known race
        } catch (Exception e) {
            logger.warn("unexpected exception performing scheduled flush", e);
        } finally {
            // Do not schedule another flush if closed
            if (isClosed.get() == false) {
                cancellableFlushTask = engineConfig.getThreadPool().schedule(this::scheduleFlush, nextFlushDelay, ThreadPool.Names.FLUSH);
            }
        }
    }

    // visible for testing
    void performScheduledFlush() {
        flush(false, false);
    }

    @Override
    public boolean flush(boolean force, boolean waitIfOngoing) throws EngineException {
        try (ReleasableLock locked = waitIfOngoing ? flushLock.acquire() : flushLock.tryAcquire()) {
            if (locked == null) {
                return false;
            }
            long newLastFlushNanos = relativeTimeInNanosSupplier.getAsLong();
            boolean result = super.flush(force, waitIfOngoing);
            if (result) {
                lastFlushNanos.set(newLastFlushNanos);
            }
            return result;
        }
    }

    // visible for testing
    long getLastFlushNanos() {
        return lastFlushNanos.get();
    }

    // visible for testing
    long getCurrentGeneration() {
        return getLastCommittedSegmentInfos().getGeneration();
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        if (source.equals(TransportShardRefreshAction.SOURCE_API) || source.equals(PostWriteRefresh.FORCED_REFRESH_AFTER_INDEX)) {
            flush(true, true);
        }
        return super.refresh(source);
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        super.asyncEnsureTranslogSynced(location, e -> {
            if (e != null) {
                listener.accept(e);
            } else {
                translogReplicator.sync(shardId, location, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        listener.accept(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.accept(e);
                    }
                });
            }

        });
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return super.isTranslogSyncNeeded() || translogReplicator.isSyncNeeded(shardId);
    }

    @Override
    public void syncTranslog() throws IOException {
        assert Thread.currentThread().getName().contains("[" + ThreadPool.Names.WRITE + "]") == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be on a write thread. Reason: [syncTranslog]";
        super.syncTranslog();
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        translogReplicator.syncAll(shardId, future);
        try {
            future.actionGet();
        } catch (Exception e) {
            throw new IOException("Exception while syncing translog remotely", e);
        }
    }

    @Override
    public void flushAndClose() throws IOException {
        // Don't flush on closing to avoid doing blobstore IO for reading back the latest commit from the repository
        // if it's not cached or doing an actual flush if there's outstanding translog operations.
        close();
    }

    public void close() throws IOException {
        cancellableFlushTask.cancel();
        super.close();
    }
}
