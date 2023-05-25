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

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.LiveVersionMapArchive;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@link Engine} implementation for index shards
 */
public class IndexEngine extends InternalEngine {

    public static final Setting<Boolean> INDEX_FAST_REFRESH = Setting.boolSetting(
        "index.fast_refresh",
        false,
        Setting.Property.Final,
        Setting.Property.IndexScope
    );

    private final TranslogReplicator translogReplicator;
    private final StatelessCommitService statelessCommitService;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final AtomicLong lastFlushNanos;
    private final Function<String, BlobContainer> translogBlobContainer;
    private final boolean fastRefresh;
    private volatile TimeValue indexFlushInterval;
    private volatile Scheduler.Cancellable cancellableFlushTask;
    private final ReleasableLock flushLock = new ReleasableLock(new ReentrantLock());
    private final RefreshThrottler refreshThrottler;

    public IndexEngine(
        EngineConfig engineConfig,
        TranslogReplicator translogReplicator,
        Function<String, BlobContainer> translogBlobContainer,
        StatelessCommitService statelessCommitService,
        RefreshThrottler.Factory refreshThrottlerFactory
    ) {
        super(engineConfig);
        assert engineConfig.isPromotableToPrimary();
        this.translogReplicator = translogReplicator;
        this.translogBlobContainer = translogBlobContainer;
        this.statelessCommitService = statelessCommitService;
        this.relativeTimeInNanosSupplier = config().getRelativeTimeInNanosSupplier();
        this.lastFlushNanos = new AtomicLong(relativeTimeInNanosSupplier.getAsLong());
        this.indexFlushInterval = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
        this.fastRefresh = INDEX_FAST_REFRESH.get(config().getIndexSettings().getSettings());
        this.refreshThrottler = refreshThrottlerFactory.create(this::doExternalRefresh);
        this.cancellableFlushTask = scheduleFlushTask();
    }

    @Override
    public void onSettingsChanged() {
        super.onSettingsChanged();
        this.indexFlushInterval = IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.get(config().getIndexSettings().getSettings());
        cancellableFlushTask.cancel();
        cancellableFlushTask = scheduleFlushTask();
    }

    private Scheduler.Cancellable scheduleFlushTask() {
        if (indexFlushInterval.getMillis() > 0) {
            return engineConfig.getThreadPool().schedule(this::scheduleFlush, indexFlushInterval, ThreadPool.Names.FLUSH);
        } else {
            return new Scheduler.Cancellable() {

                private volatile boolean isCancelled;

                @Override
                public boolean cancel() {
                    boolean thisCancelled = isCancelled == false;
                    isCancelled = true;
                    return thisCancelled;
                }

                @Override
                public boolean isCancelled() {
                    return isCancelled;
                }
            };
        }
    }

    @Override
    public boolean refreshNeeded() {
        // TODO: maybe read the routingEntry.isSearchable()?
        if (fastRefresh) {
            return super.refreshNeeded();
        } else {
            return false;
        }
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
        flush(false, false, ActionListener.noop());
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        try (ReleasableLock locked = waitIfOngoing ? flushLock.acquire() : flushLock.tryAcquire()) {
            if (locked == null) {
                listener.onResponse(FlushResult.NO_FLUSH);
            }
            long newLastFlushNanos = relativeTimeInNanosSupplier.getAsLong();
            long generationBeforeFlush = getLastCommittedSegmentInfos().getGeneration();
            super.flush(force, waitIfOngoing, listener);
            // If the generation advanced a new commit was made with this call.
            if (getLastCommittedSegmentInfos().getGeneration() > generationBeforeFlush) {
                lastFlushNanos.set(newLastFlushNanos);
            }
        }
    }

    @Override
    protected RefreshResult refreshInternalSearcher(String source, boolean block) throws EngineException {
        if (source.equals(REAL_TIME_GET_REFRESH_SOURCE) || source.equals(UNSAFE_VERSION_MAP_REFRESH_SOURCE)) {
            // TODO: Eventually the Refresh API will also need to transition (maybe) to an async API here.
            flush(true, true);
        }
        // TODO: could we avoid this refresh if we have flushed above?
        return super.refreshInternalSearcher(source, block);
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
    public void externalRefresh(String source, ActionListener<RefreshResult> listener) {
        // TODO: should we first check if a flush/refresh is needed or not? If not we could simply not go
        // through the throttler.
        refreshThrottler.maybeThrottle(new RefreshThrottler.Request(source, listener));
    }

    private void doExternalRefresh(RefreshThrottler.Request request) {
        flush(true, true, request.listener().delegateFailure((l, flushResult) -> super.externalRefresh(request.source(), l)));
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

    @Override
    public LiveVersionMapArchive createLiveVersionMapArchive() {
        return new StatelessLiveVersionMapArchive(this::getPreCommitSegmentGeneration);
    }

    public void commitSuccess(long generation) {
        ((StatelessLiveVersionMapArchive) getLiveVersionMapArchive()).afterUnpromotablesRefreshed(generation);
    }

    @Override
    protected Translog.Snapshot newTranslogSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
        SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(this.store.directory());
        Optional<String> nodeEphemeralId = searchDirectory.getCurrentMetadataNodeEphemeralId();
        if (nodeEphemeralId.isPresent()) {
            logger.debug("new translog snapshot seqnos [{}]-[{}] and node ephemeral id [{}]", fromSeqNo, toSeqNo, nodeEphemeralId.get());
            BlobContainer translogBlobContainer = this.translogBlobContainer.apply(nodeEphemeralId.get());
            TranslogReplicatorReader reader = new TranslogReplicatorReader(translogBlobContainer, shardId, fromSeqNo, toSeqNo);
            return new Translog.Snapshot() {
                @Override
                public int totalOperations() {
                    return reader.totalOperations();
                }

                @Override
                public Translog.Operation next() throws IOException {
                    Translog.Operation next = reader.next();
                    if (next != null) {
                        advanceMaxSeqNoOfUpdatesOrDeletes(next.seqNo());
                    }
                    return next;
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
        } else {
            return Translog.Snapshot.EMPTY;
        }
    }

    @Override
    protected void waitForCommitDurability(long generation, ActionListener<Void> listener) {
        try {
            ensureOpen();
        } catch (AlreadyClosedException e) {
            listener.onFailure(e);
            return;
        }
        if (getLastCommittedSegmentInfos().getGeneration() < generation) {
            listener.onFailure(new IllegalStateException("Cannot wait on generation which has not been committed"));
        } else {
            statelessCommitService.addOrNotify(shardId, generation, listener);
        }
    }
}
