/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.parquet.write.ParquetFormatSettings;
import org.elasticsearch.xpack.parquet.write.ParquetThreadPools;
import org.elasticsearch.xpack.parquet.write.compaction.ParquetCompactor;
import org.elasticsearch.xpack.parquet.write.file.BatchParquetWriter;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.MultiValuedFields;
import org.elasticsearch.xpack.parquet.write.flush.ParquetFlusher;
import org.elasticsearch.xpack.parquet.write.flush.ShardCommitData;
import org.elasticsearch.xpack.parquet.write.staging.StagedFileAdopter;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An engine that stores the documents of a shard only as Parquet files, and accepts nothing but batches of documents with
 * auto-generated ids. It has no {@code IndexWriter}, no translog of operations, no replicas and nothing to search.
 *
 * <p>The server still needs one Lucene commit with no segments and an empty translog checkpoint file as the metadata record of the
 * shard, and the engine leaves both alone. The sequence numbers live in the names of the local Parquet files: a shard bulk writes its
 * documents to one file and returns when the file is durable, so the file takes the place of the translog sync.
 *
 * <p>A flush merges the complete local files and uploads them as files of an Iceberg table in the repository, see {@link ParquetFlusher}.
 * It runs when the flush API asks for it, when the local files reach a size or an age (the periodic flush of the shard), and when the
 * shard is idle. The engine does not upload when it closes: the local files are durable, and the next start finds them.
 *
 * <p>This is a prototype.
 */
public class ParquetWriteEngine extends Engine {

    private static final String NOT_SUPPORTED = "the Parquet write engine accepts only batches of documents with auto-generated ids";

    private final Object writeLock = new Object();
    private final LocalCheckpointTracker tracker;
    private final String historyUUID;
    private final AtomicLong localRows = new AtomicLong();
    private final AtomicLong localBytes = new AtomicLong();
    private final AtomicLong uploadedRows = new AtomicLong();
    private final AtomicLong flushGeneration = new AtomicLong();
    private final ShardTables tables;
    private final ReentrantLock flushLock = new ReentrantLock();
    /**
     * The flushes that were started and have not completed. The shard schedules a periodic flush and releases its own guard as soon
     * as {@code flush} returns, because the flush of the usual engine is done by then. Ours completes later, so without this the shard
     * would start a new flush for the same local files in a loop for as long as the first one runs.
     */
    private final AtomicInteger activeFlushes = new AtomicInteger();
    /** The flusher, created at the first flush because it needs the repository. */
    private volatile ParquetFlusher flusher;
    /** The table of the index, opened with the flusher; the compactor works on the same table. */
    private volatile ShardTable table;
    private final Object tableLock = new Object();
    /** True once the staged files of the lost stores of this shard have been adopted (or there was nothing to adopt). */
    private final AtomicBoolean stagedFilesAdopted = new AtomicBoolean();
    /** True while an adoption runs: only one may. */
    private final AtomicBoolean adopting = new AtomicBoolean();
    /** The timer that looks for files to compact when no flush asks for it. */
    private volatile Scheduler.Cancellable compactionTimer;
    /** True while a compaction of this shard runs: only one may, and a flush that uploads again while it runs does not start another. */
    private final AtomicBoolean compacting = new AtomicBoolean();
    /** Set by each flush that uploaded files, and cleared when a compaction starts, so that no upload waits for the next flush. */
    private final AtomicBoolean compactionRequested = new AtomicBoolean();
    /** When the oldest local file that is not uploaded was written, in {@code System.nanoTime()}, or 0 when there is none. */
    private volatile long oldestLocalNanos;
    private final List<ReferenceManager.RefreshListener> externalRefreshListeners;
    private final List<ReferenceManager.RefreshListener> internalRefreshListeners;
    private volatile Translog.Location lastWriteLocation = new Translog.Location(0, 0, 0);

    @SuppressWarnings("this-escape")
    public ParquetWriteEngine(EngineConfig config, ShardTables tables) {
        super(config);
        this.tables = tables;
        this.externalRefreshListeners = config.getExternalRefreshListener();
        this.internalRefreshListeners = config.getInternalRefreshListener();
        store.incRef();
        boolean success = false;
        try {
            final Directory directory = store.directory();
            final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
            this.historyUUID = userData.get(HISTORY_UUID_KEY);
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
            LocalParquetFiles.deleteTemporaryFiles(directory);
            // The sequence numbers of the server in the commit are never changed (they stay at their start value), so the engine
            // finds its own: the largest end of a range that was uploaded, and of the local files that are not uploaded yet.
            final long uploadedThrough = ShardCommitData.uploadedThroughSeqNo(userData);
            uploadedRows.set(ShardCommitData.uploadedRows(userData));
            long maxSeqNo = Math.max(commitInfo.maxSeqNo(), uploadedThrough);
            for (LocalParquetFiles.LocalFile file : LocalParquetFiles.list(directory)) {
                maxSeqNo = Math.max(maxSeqNo, file.toSeqNo());
                if (file.toSeqNo() > uploadedThrough) {
                    // a file at or below the high-water mark is a leftover of a crash after the upload; the next flush deletes it
                    localRows.addAndGet(file.rows());
                    localBytes.addAndGet(file.length());
                }
            }
            if (localRows.get() > 0) {
                oldestLocalNanos = System.nanoTime(); // the age of a file before the restart is not known: count from now
            }
            // Every sequence number up to the maximum was acknowledged and is in a local file or was uploaded.
            this.tracker = new LocalCheckpointTracker(maxSeqNo, maxSeqNo);
            this.lastWriteLocation = new Translog.Location(0, Math.max(maxSeqNo, 0), 0);
            this.compactionTimer = config.getThreadPool()
                .scheduleWithFixedDelay(
                    this::compactionTick,
                    ParquetFormatSettings.COMPACTION_INTERVAL.get(config.getIndexSettings().getSettings()),
                    config.getThreadPool().executor(ParquetThreadPools.COMPACTION)
                );
            success = true;
            scheduleAdoption();
        } catch (IOException e) {
            throw new EngineCreationFailureException(shardId, "failed to open the Parquet write engine", e);
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    // ---- writes ----

    @Override
    public List<IndexResult> indexBatch(EngineBatch engineBatch) throws IOException {
        ensureOpen();
        final IndexOperationBatch operations = engineBatch.batch();
        if (operations.origin() != Operation.Origin.PRIMARY) {
            throw new IllegalStateException(
                "the Parquet write engine has no replicas, but got a batch of origin [" + operations.origin() + "]"
            );
        }
        final int docCount = operations.docCount();
        final boolean[] accepted = new boolean[docCount];
        int acceptedCount = 0;
        for (int i = 0; i < docCount; i++) {
            if (operations.autoGeneratedIdTimestamp(i) != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
                accepted[i] = true;
                acceptedCount++;
            }
        }
        // With staged durability the repository must work before anything is written: a missing repository, or a table that another index
        // owns, fails the bulk and leaves no local file and no gap in the sequence numbers.
        final ShardTable stagingTable = acceptedCount > 0 && isStaged() ? openTableForStaging() : null;
        LocalParquetFiles.LocalFile written = null;
        final long firstSeqNo;
        synchronized (writeLock) {
            ensureOpen();
            final long maxLocalBytes = ParquetFormatSettings.MAX_LOCAL_SIZE.get(engineConfig.getIndexSettings().getSettings()).getBytes();
            if (acceptedCount > 0 && localBytes.get() >= maxLocalBytes) {
                // The upload does not keep up, or the repository is down. A 429 tells the client to retry, and the rows are not lost.
                throw new EsRejectedExecutionException(
                    "the local Parquet files of shard ["
                        + shardId
                        + "] hold ["
                        + localBytes.get()
                        + "] bytes, which reaches [index.parquet.max_local_size]; they are uploaded by the next flush",
                    false
                );
            }
            firstSeqNo = tracker.getMaxSeqNo() + 1;
            if (acceptedCount > 0) {
                final MappingLookup lookup = engineConfig.getMapperService().mappingLookup();
                final ColumnBatch columns = engineBatch.columns().toColumnBatch();
                final LocalParquetFiles.LocalFile file;
                try {
                    file = BatchParquetWriter.write(
                        store.directory(),
                        columns,
                        lookup,
                        accepted,
                        firstSeqNo,
                        CompressionCodecName.valueOf(
                            ParquetFormatSettings.LOCAL_COMPRESSION.get(engineConfig.getIndexSettings().getSettings()).name()
                        ),
                        multiValuedFields()
                    );
                } catch (IOException e) {
                    failEngine("failed to write a Parquet file", e);
                    throw e;
                } catch (LinkageError e) {
                    // A deployment problem (a class that is missing or fails to initialize, a native library that does not load) and not a
                    // broken shard, so the engine stays open. The error must become an exception here: nothing above this call completes
                    // the bulk when an Error is thrown on the write thread.
                    throw LinkageFailures.asException("write a Parquet file", e);
                }
                // Only now, with the file durable, are the sequence numbers taken: a failed write leaves no gap.
                final long reserved = tracker.generateSeqNos(acceptedCount);
                assert reserved == firstSeqNo : reserved + " != " + firstSeqNo;
                for (long seqNo = firstSeqNo; seqNo < firstSeqNo + acceptedCount; seqNo++) {
                    tracker.markSeqNoAsProcessed(seqNo);
                    if (stagingTable == null) {
                        tracker.markSeqNoAsPersisted(seqNo);
                    }
                }
                written = file;
                localRows.addAndGet(file.rows());
                localBytes.addAndGet(file.length());
                if (oldestLocalNanos == 0) {
                    oldestLocalNanos = System.nanoTime();
                }
                lastWriteNanos = System.nanoTime();
                lastWriteLocation = new Translog.Location(0, firstSeqNo + acceptedCount - 1, 0);
            }
        }
        if (stagingTable != null && written != null) {
            // outside the lock, so that the uploads of concurrent bulks of the shard overlap; the bulk is acknowledged after this returns
            stage(stagingTable, written, acceptedCount);
        }
        final List<IndexResult> results = new ArrayList<>(docCount);
        long seqNo = firstSeqNo;
        for (int i = 0; i < docCount; i++) {
            if (accepted[i]) {
                results.add(new ParquetIndexResult(operations.primaryTerm(), seqNo, operations.id(i), new Translog.Location(0, seqNo, 0)));
                seqNo++;
            } else {
                results.add(
                    new IndexResult(new IllegalArgumentException(NOT_SUPPORTED + ": the document has an explicit id"), 1L, operations.id(i))
                );
            }
        }
        return results;
    }

    /**
     * The table of the index for a bulk with staged durability. A failure is the failure of the bulk, with a status that tells the
     * client to retry: the shard is fine, the repository is not.
     */
    private ShardTable openTableForStaging() {
        try {
            return openTable();
        } catch (IOException | RuntimeException e) {
            throw new ElasticsearchStatusException(
                "cannot stage the bulk for shard [{}]: the repository [{}] or the table is not usable",
                RestStatus.SERVICE_UNAVAILABLE,
                e,
                shardId,
                ParquetFormatSettings.REPOSITORY.get(engineConfig.getIndexSettings().getSettings())
            );
        } catch (LinkageError e) {
            throw LinkageFailures.asException("open the table to stage a bulk", e);
        }
    }

    /**
     * Copies the local file of a bulk to the staging directory of the repository, and only then marks its sequence numbers persisted.
     * A failure discards the bulk: the local file is deleted (its sequence numbers stay used, which is a gap that the engine tolerates,
     * because the ranges are in the names of the files and the rows come from the footers), and the client gets an error and may retry.
     * A flush can have picked the local file up already. Then its rows are in the table, and they are durable.
     */
    private void stage(ShardTable table, LocalParquetFiles.LocalFile file, int rows) {
        final String attempt = ShardTable.newAttempt();
        String stagedName = null;
        try {
            try (IndexInput in = store.directory().openInput(file.name(), IOContext.DEFAULT)) {
                stagedName = table.stageFile(
                    file.fromSeqNo(),
                    file.toSeqNo(),
                    attempt,
                    new BufferedInputStream(new InputStreamIndexInput(in, file.length())),
                    file.length()
                );
            } catch (NoSuchFileException e) {
                // flushed already: the rows are in the table
            }
        } catch (IOException | RuntimeException | LinkageError e) {
            discard(table, file, stagedName != null ? stagedName : ShardTable.dataFileName(file.fromSeqNo(), file.toSeqNo(), attempt));
            markPersisted(file.fromSeqNo(), rows);
            if (e instanceof LinkageError linkage) {
                throw LinkageFailures.asException("stage a bulk", linkage);
            }
            throw new ElasticsearchStatusException(
                "could not stage the bulk for shard [{}] in the repository, so it is not acknowledged",
                RestStatus.SERVICE_UNAVAILABLE,
                e,
                shardId
            );
        }
        markPersisted(file.fromSeqNo(), rows);
    }

    private void markPersisted(long firstSeqNo, int rows) {
        synchronized (writeLock) {
            for (long seqNo = firstSeqNo; seqNo < firstSeqNo + rows; seqNo++) {
                tracker.markSeqNoAsPersisted(seqNo);
            }
        }
    }

    /** Best effort: the local file of a bulk that failed to stage, and the blob that may have been written. */
    private void discard(ShardTable table, LocalParquetFiles.LocalFile file, String stagedName) {
        try {
            store.directory().deleteFile(file.name());
        } catch (IOException e) {
            // already gone (a flush took it), or it stays and a flush uploads it: the client got an error, so it may have retried
            logger.debug(() -> Strings.format("could not delete the local file [%s] of a bulk that was not staged", file.name()), e);
        }
        try {
            table.deleteStaged(List.of(stagedName));
        } catch (IOException | RuntimeException e) {
            logger.debug(() -> Strings.format("could not delete the staged file [%s] of a bulk that was not acknowledged", stagedName), e);
        }
        try {
            refreshLocalFiles();
        } catch (IOException e) {
            logger.debug("could not count the local files after a bulk that was not staged", e);
        }
    }

    /**
     * A successful result that carries a synthetic translog location. The server asserts that every acknowledged write has a location
     * that increases, and a result of another package cannot set it, so the location is the sequence number of the document.
     */
    private static final class ParquetIndexResult extends IndexResult {
        private final Translog.Location location;

        ParquetIndexResult(long term, long seqNo, String id, Translog.Location location) {
            super(1L, term, seqNo, true, id);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return location;
        }
    }

    @Override
    public IndexResult index(Index index) {
        return new IndexResult(new UnsupportedOperationException(NOT_SUPPORTED), index.version(), index.id());
    }

    @Override
    public DeleteResult delete(Delete delete) {
        return new DeleteResult(
            new UnsupportedOperationException("the Parquet write engine does not delete documents"),
            delete.version(),
            delete.primaryTerm(),
            delete.id()
        );
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        synchronized (writeLock) {
            tracker.advanceMaxSeqNo(noOp.seqNo());
            tracker.markSeqNoAsProcessed(noOp.seqNo());
            tracker.markSeqNoAsPersisted(noOp.seqNo());
        }
        return new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        SplitShardCountSummary splitShardCountSummary,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        throw notReadable();
    }

    @Override
    public GetResult getForUpdate(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        throw new UnsupportedOperationException("the Parquet write engine does not update documents");
    }

    @Override
    public GetResult getFromTranslog(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return null;
    }

    private IllegalArgumentException notReadable() {
        return new IllegalArgumentException(
            "index [" + shardId.getIndexName() + "] uses the Parquet write engine and cannot be read or searched"
        );
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        throw notReadable();
    }

    // ---- refresh and flush: nothing is searchable, but the listeners of the shard must still run ----

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        try {
            for (ReferenceManager.RefreshListener listener : externalRefreshListeners) {
                listener.beforeRefresh();
            }
            for (ReferenceManager.RefreshListener listener : internalRefreshListeners) {
                listener.beforeRefresh();
            }
            for (ReferenceManager.RefreshListener listener : externalRefreshListeners) {
                listener.afterRefresh(false);
            }
            for (ReferenceManager.RefreshListener listener : internalRefreshListeners) {
                listener.afterRefresh(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new RefreshResult(false, config().getPrimaryTermSupplier().getAsLong(), 0);
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        ActionListener.completeWith(listener, () -> refresh(source));
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public void writeIndexingBuffer() {}

    /** True when the local files have reached the size or the age at which they are uploaded. */
    @Override
    public boolean shouldPeriodicallyFlush() {
        final long bytes = localBytes.get();
        if (bytes == 0 || activeFlushes.get() > 0) {
            return false;
        }
        final Settings settings = engineConfig.getIndexSettings().getSettings();
        if (bytes >= ParquetFormatSettings.FLUSH_THRESHOLD_SIZE.get(settings).getBytes()) {
            return true;
        }
        final long oldest = oldestLocalNanos;
        return oldest != 0 && System.nanoTime() - oldest >= ParquetFormatSettings.FLUSH_THRESHOLD_AGE.get(settings).nanos();
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) throws EngineException {
        // The flush is blocking I/O for a long time, so it runs on a pool of its own (see ParquetThreadPools) and completes the listener
        // from there. It must not run on the pool of the caller: the verification before the close of an index blocks a generic thread
        // on this listener.
        if (activeFlushes.incrementAndGet() > 1 && waitIfOngoing == false) {
            activeFlushes.decrementAndGet();
            listener.onResponse(new FlushResult(true, flushGeneration.get()));
            return;
        }
        final ActionListener<FlushResult> done = ActionListener.runAfter(listener, activeFlushes::decrementAndGet);
        engineConfig.getThreadPool().executor(ParquetThreadPools.FLUSH).execute(ActionRunnable.wrap(done, l -> {
            try (var ignored = acquireEnsureOpenRef()) {
                ActionListener.completeWith(l, () -> {
                    if (waitIfOngoing) {
                        flushLock.lock();
                    } else if (flushLock.tryLock() == false) {
                        return new FlushResult(true, flushGeneration.get());
                    }
                    try {
                        flushLocalFiles();
                        return new FlushResult(false, flushGeneration.incrementAndGet());
                    } finally {
                        flushLock.unlock();
                    }
                });
            }
        }));
    }

    /** Merges the complete local files, uploads them and lists them in the table. Runs under the flush lock. */
    private void flushLocalFiles() throws IOException {
        try {
            ParquetFlusher current = flusher;
            if (current == null) {
                current = new ParquetFlusher(
                    store.directory(),
                    new StoreCommitData(store),
                    openTable(),
                    this::flushSettings,
                    ParquetFlusher.Hooks.NONE
                );
                flusher = current;
            }
            final long start = System.nanoTime();
            final ParquetFlusher.Result result = current.flush();
            if (result.files() > 0) {
                logger.info(
                    "flushed [{}] files with [{}] rows ([{}]) to the repository in [{}]",
                    result.files(),
                    result.rows(),
                    ByteSizeValue.ofBytes(result.bytes()),
                    TimeValue.timeValueNanos(System.nanoTime() - start)
                );
                scheduleCompaction();
            }
        } catch (LinkageError e) {
            // see the comment at the write: an Error here would never complete the listener of the flush
            throw LinkageFailures.asException("flush the local Parquet files", e);
        }
        refreshLocalFiles();
    }

    /** The mapping source and the fields of it that are lists, for the last mapping that a batch saw. */
    private record MultiValued(CompressedXContent source, Set<String> fields) {}

    private volatile MultiValued multiValued;

    /** The fields of the current mapping that are lists. A mapping update gives a new source, and then the set is made again. */
    private Set<String> multiValuedFields() {
        final CompressedXContent source = engineConfig.getMapperService().documentMapper().mappingSource();
        final MultiValued known = multiValued;
        if (known != null && known.source().equals(source)) {
            return known.fields();
        }
        final Set<String> fields = MultiValuedFields.of(
            source,
            FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.get(engineConfig.getIndexSettings().getSettings())
        );
        multiValued = new MultiValued(source, fields);
        return fields;
    }

    /**
     * Starts the adoption of the staged files of lost stores, once, on the flush pool. It is called when the engine starts and by every
     * tick of the timer until it has succeeded, because the repository can be unavailable at the start. Only for staged durability.
     */
    private void scheduleAdoption() {
        if (isStaged() == false || stagedFilesAdopted.get() || isClosed.get() || adopting.compareAndSet(false, true) == false) {
            return;
        }
        try {
            engineConfig.getThreadPool().executor(ParquetThreadPools.FLUSH).execute(() -> {
                try {
                    adoptStagedFiles();
                } finally {
                    adopting.set(false);
                }
            });
        } catch (RuntimeException e) {
            adopting.set(false); // the pool is shut down
            logger.debug("could not schedule the adoption of staged files", e);
        }
    }

    private void adoptStagedFiles() {
        if (store.tryIncRef() == false) {
            return;
        }
        try {
            final StagedFileAdopter.Result result = new StagedFileAdopter(
                store.directory(),
                openTable(),
                this::flushSettings,
                isClosed::get,
                StagedFileAdopter.Hooks.NONE
            ).adopt();
            if (result.files() > 0) {
                logger.info(
                    "adopted [{}] staged files with [{}] rows of [{}] lost histories of shard [{}]",
                    result.files(),
                    result.rows(),
                    result.histories(),
                    shardId
                );
            }
            if (isClosed.get() == false) {
                stagedFilesAdopted.set(true);
            }
        } catch (LinkageError e) {
            logger.warn("adoption of staged files failed", LinkageFailures.asException("adopt the staged files", e));
        } catch (Exception e) {
            // for example a repository that is not available yet: the next tick of the timer tries again
            logger.warn("adoption of staged files failed; the next tick tries again", e);
        } finally {
            store.decRef();
        }
    }

    /** The table of the index, opened at the first use: by a flush, or by the compaction timer after a restart. */
    private ShardTable openTable() throws IOException {
        ShardTable opened = table;
        if (opened == null) {
            synchronized (tableLock) {
                opened = table;
                if (opened == null) {
                    opened = tables.open(shardId, engineConfig.getIndexSettings().getSettings(), historyUUID);
                    table = opened;
                }
            }
        }
        return opened;
    }

    /**
     * A tick of the compaction timer. A compaction normally starts after a flush that uploaded files, and that is not enough when the
     * shard has just restarted, when a compaction was dropped, or when a shard receives no more writes. After a restart no flush has
     * opened the table yet, so the tick opens it, but only for a shard that has uploaded something: an index that has never flushed has no
     * table to compact, and should not get one from a timer.
     */
    private void compactionTick() {
        scheduleAdoption(); // not only for compaction: the repository may have been down when the engine started
        if (isClosed.get() || ParquetFormatSettings.COMPACTION_ENABLED.get(engineConfig.getIndexSettings().getSettings()) == false) {
            return;
        }
        try {
            if (table == null) {
                if (store.tryIncRef() == false) {
                    return;
                }
                try {
                    if (ShardCommitData.uploadedThroughSeqNo(new StoreCommitData(store).userData()) < 0) {
                        return;
                    }
                } finally {
                    store.decRef();
                }
                openTable();
            }
            scheduleCompaction();
        } catch (Exception e) {
            // for example a repository that is not available yet: the next tick tries again
            logger.debug("the compaction timer could not start a compaction", e);
        }
    }

    /**
     * Asks for a compaction of the uploaded files, when the index wants one. The compaction runs on the generic pool and only one runs at
     * a time. A request that arrives while one runs is not lost: the running one looks again before it ends, because the flush that made
     * the request may have uploaded files that its own table listing did not see.
     */
    private void scheduleCompaction() {
        if (table == null || ParquetFormatSettings.COMPACTION_ENABLED.get(engineConfig.getIndexSettings().getSettings()) == false) {
            return;
        }
        compactionRequested.set(true);
        startCompactionIfIdle();
    }

    private void startCompactionIfIdle() {
        if (compacting.compareAndSet(false, true) == false) {
            return; // the running compaction sees the request
        }
        engineConfig.getThreadPool().executor(ParquetThreadPools.COMPACTION).execute(() -> {
            try {
                while (compactionRequested.getAndSet(false)) {
                    compactOnce();
                }
            } finally {
                compacting.set(false);
            }
            if (compactionRequested.get()) {
                startCompactionIfIdle(); // a request that came between the last look and the release
            }
        });
    }

    /**
     * One compaction. It keeps its own reference to the store, so a shard that closes meanwhile does not close the directory under it.
     * Its failure is logged and not more: the files stay as they are, and the next flush asks again.
     */
    private void compactOnce() {
        final ShardTable shardTable = table;
        try {
            if (store.tryIncRef() == false) {
                return;
            }
            try {
                final ParquetCompactor.Result result = new ParquetCompactor(
                    store.directory(),
                    shardTable,
                    this::compactionSettings,
                    ParquetCompactor.Hooks.NONE
                ).compact();
                if (result.runs() > 0 || result.orphansDeleted() > 0) {
                    logger.debug(
                        "compacted [{}] files into [{}] ones with [{}] rows, deleted [{}] orphans",
                        result.replacedFiles(),
                        result.runs(),
                        result.rows(),
                        result.orphansDeleted()
                    );
                }
            } finally {
                store.decRef();
            }
        } catch (LinkageError e) {
            logger.warn("compaction of the Parquet files failed", LinkageFailures.asException("compact the Parquet files", e));
        } catch (Exception e) {
            logger.warn("compaction of the Parquet files failed", e);
        }
    }

    private ParquetCompactor.Settings compactionSettings() {
        final Settings settings = engineConfig.getIndexSettings().getSettings();
        return new ParquetCompactor.Settings(
            ParquetFormatSettings.COMPACTION_TARGET_FILE_SIZE.get(settings).getBytes(),
            ParquetFormatSettings.COMPACTION_MIN_FILES.get(settings),
            ParquetFormatSettings.MAX_FILE_SIZE.get(settings).getBytes(),
            ParquetFormatSettings.ROW_GROUP_SIZE.get(settings).getBytes(),
            CompressionCodecName.valueOf(ParquetFormatSettings.COMPRESSION.get(settings).name()),
            ParquetFormatSettings.COMPACTION_DELETE_DELAY.get(settings).millis()
        );
    }

    private ParquetFlusher.Settings flushSettings() {
        final Settings settings = engineConfig.getIndexSettings().getSettings();
        return new ParquetFlusher.Settings(
            ParquetFormatSettings.MAX_FILE_SIZE.get(settings).getBytes(),
            ParquetFormatSettings.ROW_GROUP_SIZE.get(settings).getBytes(),
            CompressionCodecName.valueOf(ParquetFormatSettings.COMPRESSION.get(settings).name()),
            isStaged()
        );
    }

    /** Whether a bulk is acknowledged only after a copy of its file is in the repository. */
    private boolean isStaged() {
        return ParquetFormatSettings.DURABILITY.get(
            engineConfig.getIndexSettings().getSettings()
        ) == ParquetFormatSettings.Durability.STAGED;
    }

    /**
     * The counters after a flush, from the directory: bulks that were written during the flush are in it, and the uploaded files are
     * not.
     */
    private void refreshLocalFiles() throws IOException {
        final Map<String, String> userData = new StoreCommitData(store).userData();
        final long uploadedThrough = ShardCommitData.uploadedThroughSeqNo(userData);
        synchronized (writeLock) {
            long rows = 0;
            long bytes = 0;
            for (LocalParquetFiles.LocalFile file : LocalParquetFiles.list(store.directory())) {
                if (file.toSeqNo() > uploadedThrough) {
                    rows += file.rows();
                    bytes += file.length();
                }
            }
            localRows.set(rows);
            localBytes.set(bytes);
            uploadedRows.set(ShardCommitData.uploadedRows(userData));
            oldestLocalNanos = rows == 0 ? 0 : System.nanoTime();
        }
    }

    /** Local files are durable, so closing the engine does not upload them. The next start finds them. */
    @Override
    public void flushAndClose() throws IOException {
        close();
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) {}

    // ---- sequence numbers, checkpoints and the translog that does not exist ----

    @Override
    public long getMaxSeqNo() {
        return tracker.getMaxSeqNo();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return tracker.getProcessedCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return tracker.getPersistedCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(tracker.getMaxSeqNo(), tracker.getProcessedCheckpoint(), globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return engineConfig.getGlobalCheckpointSupplier().getAsLong();
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        // Nothing is ever updated or deleted.
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {}

    @Override
    public void maybePruneDeletes() {}

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return 0;
    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) {
        return 0;
    }

    @Override
    public void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public void prepareForEngineReset() {}

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        // The Parquet file of the bulk was synced before the bulk returned.
        listener.accept(null);
    }

    @Override
    public void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void syncTranslog() {}

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) {
        return 0;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        long maxChunkSize
    ) {
        return Translog.Snapshot.EMPTY;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return tracker.getProcessedCheckpoint() + 1;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return new TranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return lastWriteLocation;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) {}

    @Override
    public void trimUnreferencedTranslogFiles() {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() {}

    @Override
    public long getLastUnsafeSegmentGenerationForGets() {
        return 0;
    }

    // ---- commits and stats: the one empty Lucene commit is the metadata record of the shard ----

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        try {
            return store.readLastCommittedSegmentsInfo();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(tracker.getProcessedCheckpoint(), 0);
    }

    /** The prototype hands out the last commit without protecting it: nothing in this engine deletes commits, and nothing snapshots it. */
    private IndexCommitRef lastCommitRef() {
        try {
            final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            return new IndexCommitRef(commits.get(commits.size() - 1), () -> {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return lastCommitRef();
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return lastCommitRef();
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public DocsStats docStats() {
        return new DocsStats(uploadedRows.get() + localRows.get(), 0, localBytes.get());
    }

    @Override
    public ShardFieldStats shardFieldStats() {
        return new ShardFieldStats(0, 0, 0, 0, 0, 0);
    }

    @Override
    public FieldInfos shardFieldInfos() {
        return FieldInfos.EMPTY;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats();
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return new SegmentsStats();
    }

    @Override
    public List<Segment> segments() {
        return List.of();
    }

    @Override
    public List<Segment> segments(boolean includeVectorFormatsInfo, boolean includeAutoCalibration) {
        return List.of();
    }

    @Override
    public DenseVectorStats denseVectorStats(MappingLookup mappingLookup) {
        return new DenseVectorStats();
    }

    @Override
    public SparseVectorStats sparseVectorStats(MappingLookup mappingLookup) {
        return new SparseVectorStats();
    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) {
        return ShardLongFieldRange.UNKNOWN;
    }

    // ---- throttling and close ----

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public void suspendThrottling() {}

    @Override
    public void resumeThrottling() {}

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                final Scheduler.Cancellable timer = compactionTimer;
                if (timer != null) {
                    timer.cancel();
                }
                IOUtils.close(store::decRef);
            } catch (Exception e) {
                logger.warn("failed to close the Parquet write engine", e);
            } finally {
                closedLatch.countDown();
            }
        }
    }
}
