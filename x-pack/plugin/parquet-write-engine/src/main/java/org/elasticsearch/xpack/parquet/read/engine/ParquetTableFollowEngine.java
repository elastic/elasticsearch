/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.read.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.ElasticsearchReaderManager;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.parquet.read.dv.ColumnBinding;
import org.elasticsearch.xpack.parquet.read.dv.Conversion;
import org.elasticsearch.xpack.parquet.read.dv.ExternalFilePiece;
import org.elasticsearch.xpack.parquet.read.dv.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.read.dv.ParquetReadContext;
import org.elasticsearch.xpack.parquet.read.repository.FileLocation;
import org.elasticsearch.xpack.parquet.read.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.read.repository.ParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.BatchParquetWriter;
import org.elasticsearch.xpack.parquet.write.table.TableReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A read-only engine that follows an Iceberg table: its segments are stubs that reference the data files of the table (see
 * {@link ExternalParquetCodecReader}), and a reload brings them up to date with the current snapshot of the table. The search replicas
 * of an index of the write engine run it over the files of their shard, and an index that mounts a table runs it over its share of
 * the files of the table.
 *
 * <p>The shard's own store holds the stubs, one segment for each data file, in the order that the {@link FileSelector} gives. A reload
 * that finds only new files appends their stubs, so the readers of the other segments (and their caches) stay; a reload that finds a
 * file gone (a compaction replaced it) or a change of the mapping builds the stubs again. The commit of the store records which files
 * its stubs reference, so a shard that starts again continues from there. A copy of a table has nothing that the table does not have,
 * so the store holds no state that matters: it is rebuilt whenever it disagrees with the table.
 *
 * <p>A reload happens on every refresh of the shard, the scheduled one ({@code index.refresh_interval}) included, and when the primary
 * asks for a version of the table: the primary of an index of the write engine announces each version that it commits, and answers a
 * refresh of the index with the version that holds its uploads, and the server forwards that version to this engine through
 * {@link #addPrimaryTermAndGenerationListener}, which completes once the engine has read that version or a later one.
 *
 * <p>The {@code _id} of a document is its row number in the shard, as in {@link ParquetReadOnlyEngine}: a compaction or a file of
 * another history can change it.
 */
public final class ParquetTableFollowEngine extends Engine {

    private static final Logger logger = LogManager.getLogger(ParquetTableFollowEngine.class);

    /** The data files that the stubs of the last commit reference, one location for each line, in the order of the segments. */
    static final String FILES_KEY = "parquet.follow.files";
    /** The fields that the stubs of the last commit bind, comma separated: the stubs are built again when the mapping changes. */
    static final String FIELDS_KEY = "parquet.follow.fields";

    /** Chooses the data files of the table that the shard serves, in the order of their rows. */
    @FunctionalInterface
    public interface FileSelector {
        List<TableReader.DataFileEntry> select(TableReader.Contents contents);
    }

    /**
     * The fields that the stubs serve, and the column of each. A field whose column a file lacks gets no binding in the stub of that file.
     * A conversion of {@link Conversion#NONE} for a floating point column means the sortable encoding of the column.
     */
    @FunctionalInterface
    public interface FieldSource {
        List<ParquetBackedSettings.FieldSpec> fields(MappingLookup mappingLookup);
    }

    /**
     * The fields of an index of the write engine: each mapped field of a type that the engine writes is the column of the same name, a
     * flat column named by the full path of the field. A list field ({@code multi_value}) is a nested column and not served yet, so it
     * finds no flat column and gets no binding.
     */
    public static final FieldSource MAPPED_FIELDS = mappingLookup -> {
        final List<ParquetBackedSettings.FieldSpec> fields = new ArrayList<>();
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof FieldMapper fieldMapper
                && (fieldMapper instanceof MetadataFieldMapper) == false
                && BatchParquetWriter.SUPPORTED_FIELD_TYPES.contains(fieldMapper.typeName())) {
                fields.add(new ParquetBackedSettings.FieldSpec(fieldMapper.fullPath(), fieldMapper.fullPath(), Conversion.NONE));
            }
        }
        fields.sort(Comparator.comparing(ParquetBackedSettings.FieldSpec::field));
        return fields;
    };

    /** What the engine needs to reach the table and its files. */
    public interface TableAccess {

        /** The reader of the table, which the first call may open (it runs on a thread that may use the repository). */
        TableReader tableReader() throws IOException;

        /** Where a data file of the table is, as the stubs store it. */
        FileLocation locate(TableReader.DataFileEntry file);

        /** Opens the file that a stub references, see {@link ParquetReadContext.Resolver}. */
        ParquetReadContext.Resolver resolver();
    }

    private final TableAccess access;
    private final FileSelector selector;
    private final FieldSource fieldSource;
    private final Executor reloadExecutor;
    private final String historyUUID;
    private final ElasticsearchReaderManager readerManager;

    /** Serializes the reloads, which write the store. */
    private final Object reloadMutex = new Object();
    /** The listeners that wait for a version of the table, and the version that each waits for. Guarded by itself. */
    private final List<Tuple<Long, ActionListener<Long>>> versionListeners = new ArrayList<>();

    /** The version of the table that the last commit of the stubs holds, or -1 before the first read. */
    private volatile long committedVersion = -1;
    /** The version of the table that the current reader shows, or -1 before the first read. */
    private volatile long loadedVersion = -1;
    private volatile FieldInfos fieldInfos = FieldInfos.EMPTY;

    @SuppressWarnings("this-escape")
    public ParquetTableFollowEngine(
        EngineConfig config,
        TableAccess access,
        FileSelector selector,
        FieldSource fieldSource,
        Executor reloadExecutor
    ) {
        super(config);
        this.access = access;
        this.selector = selector;
        this.fieldSource = fieldSource;
        this.reloadExecutor = reloadExecutor;
        store.incRef();
        boolean success = false;
        ElasticsearchDirectoryReader reader = null;
        try {
            final SegmentInfos commit = store.readLastCommittedSegmentsInfo();
            this.historyUUID = commit.getUserData().get(HISTORY_UUID_KEY);
            reader = ParquetReadContext.with(
                access.resolver(),
                () -> ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId)
            );
            this.readerManager = new ElasticsearchReaderManager(reader);
            for (ReferenceManager.RefreshListener listener : config.getExternalRefreshListener()) {
                readerManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener : config.getInternalRefreshListener()) {
                readerManager.addListener(listener);
            }
            readerManager.addListener(new VersionTracker());
            fieldInfos = FieldInfos.getMergedFieldInfos(reader);
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open the stubs of " + shardId, e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(reader);
                store.decRef();
            }
        }
        // The constructor reads nothing: it runs while the shard holds its engine mutex. The refresh at the end of the recovery of the
        // shard reads the table, and until then the shard serves the stubs of its last commit.
    }

    /** The version of the table that the searches see, or -1 if the engine has not read the table yet. */
    public long loadedVersion() {
        return loadedVersion;
    }

    // ---- reload ----

    /**
     * Reads the table and brings the stubs of the store up to date with the files that the shard serves, without refreshing the reader.
     * It takes no lock of the shard: a thread of the shard can hold the engine lock of the shard while it waits for this.
     */
    void readTable() throws IOException {
        onRepositoryThread(() -> {
            readTableHere();
            return null;
        });
    }

    /**
     * Runs an action that reads the repository: a read of the table, the footers of new files, or a refresh of the reader, which opens
     * the stubs and so their files. A repository gives its blob store only to threads of the generic and snapshot pools, so a thread of
     * another pool (the refresh pool) waits for a generic thread instead, which is what the refresh pool is for.
     */
    private <T> T onRepositoryThread(CheckedSupplier<T, IOException> action) throws IOException {
        final String executor = EsExecutors.executorName(Thread.currentThread());
        if (ThreadPool.Names.GENERIC.equals(executor) || ThreadPool.Names.SNAPSHOT.equals(executor)) {
            return action.get();
        }
        final PlainActionFuture<T> future = new PlainActionFuture<>();
        engineConfig.getThreadPool().generic().execute(() -> {
            try {
                future.onResponse(action.get());
            } catch (Throwable t) {
                // an Error too: the caller waits
                future.onFailure(t instanceof Exception e ? e : new IllegalStateException("failed to read the table of " + shardId, t));
            }
        });
        try {
            return future.actionGet();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private void readTableHere() throws IOException {
        synchronized (reloadMutex) {
            ensureOpen();
            final TableReader.Contents contents = access.tableReader().read();
            updateStubs(selector.select(contents));
            committedVersion = contents.version();
        }
    }

    /**
     * Records the version of the table that a refresh of the reader shows: the version of the commit of the stubs when the refresh
     * starts, because the refresh opens that commit or a later one.
     */
    private final class VersionTracker implements ReferenceManager.RefreshListener {
        private long refreshing = -1;

        @Override
        public void beforeRefresh() {
            refreshing = committedVersion;
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            // without a change the reader already shows the last commit
            if (refreshing > loadedVersion) {
                loadedVersion = refreshing;
            }
            completeVersionListeners();
        }
    }

    private boolean updateStubs(List<TableReader.DataFileEntry> files) throws IOException {
        final List<ParquetBackedSettings.FieldSpec> fields = fieldSource.fields(engineConfig.getMapperService().mappingLookup());
        final String fieldsSignature = String.join(
            ",",
            fields.stream().map(f -> f.field() + ":" + f.column() + ":" + f.conversion()).toList()
        );
        final List<String> locations = new ArrayList<>(files.size());
        for (TableReader.DataFileEntry file : files) {
            locations.add(access.locate(file).toString());
        }

        final SegmentInfos commit = store.readLastCommittedSegmentsInfo();
        final Map<String, String> userData = commit.getUserData();
        final List<String> committedFiles = splitLines(userData.get(FILES_KEY));
        final boolean sameFields = fieldsSignature.equals(userData.getOrDefault(FIELDS_KEY, fieldsSignature));
        if (sameFields && committedFiles.equals(locations) && userData.containsKey(FIELDS_KEY)) {
            return false;
        }
        // only new files at the end: their stubs are appended, and the other segments keep their readers
        final boolean append = sameFields
            && userData.containsKey(FIELDS_KEY)
            && locations.size() >= committedFiles.size()
            && locations.subList(0, committedFiles.size()).equals(committedFiles);
        final int firstNew = append ? committedFiles.size() : 0;
        long rowBase = append ? commit.totalMaxDoc() : 0;

        final Map<String, String> newUserData = new HashMap<>(userData);
        newUserData.put(FILES_KEY, String.join("\n", locations));
        newUserData.put(FIELDS_KEY, fieldsSignature);
        final IndexWriterConfig iwc = Lucene.indexWriterConfigWithNoMerging(null)
            .setMergeScheduler(new SerialMergeScheduler())
            .setOpenMode(append ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE)
            .setCodec(new ParquetAttachCodec())
            .setCommitOnClose(false);
        final long base = rowBase;
        ParquetReadContext.with(access.resolver(), () -> {
            long nextRow = base;
            try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
                for (int i = firstNew; i < files.size(); i++) {
                    final TableReader.DataFileEntry file = files.get(i);
                    if (file.rows() == 0) {
                        // a file with no row has no row group, and a stub needs one; the commit still lists it, so it is not read again
                        continue;
                    }
                    final ParquetFileInfo info = readFooter(locations.get(i), file.length());
                    final List<ColumnBinding> bindings = bindings(fields, info);
                    final ExternalFilePiece piece = ExternalFilePiece.of(info, 0, info.rowGroups().size(), nextRow, bindings);
                    writer.addIndexes(new ExternalParquetCodecReader(bindings, piece));
                    nextRow += piece.rowCount();
                }
                writer.setLiveCommitData(newUserData.entrySet());
                writer.commit();
            }
            return null;
        });
        logger.debug(
            "{} {} the stubs of [{}] files of the table, [{}] of them new",
            shardId,
            append ? "appended to" : "rebuilt",
            files.size(),
            files.size() - firstNew
        );
        return true;
    }

    private ParquetFileInfo readFooter(String location, long length) throws IOException {
        final StorageObject object = access.resolver().open(location, length);
        try {
            return ParquetFileInfo.read(location, object);
        } finally {
            ParquetFiles.close(object);
        }
    }

    private static List<String> splitLines(String value) {
        if (value == null || value.isEmpty()) {
            return List.of();
        }
        return List.of(value.split("\n"));
    }

    /**
     * The bindings of the fields to the columns of one file. A field whose column the file does not have (the file was written before
     * a mapping update added the field) has no binding in the stub of that file, so its documents have no value. The field number of a
     * field is its position in {@code fields}, which a change of the fields replaces with a rebuild of all stubs.
     */
    private List<ColumnBinding> bindings(List<ParquetBackedSettings.FieldSpec> fields, ParquetFileInfo file) {
        final List<ColumnBinding> bindings = new ArrayList<>();
        for (int number = 0; number < fields.size(); number++) {
            final ParquetBackedSettings.FieldSpec field = fields.get(number);
            final ParquetFileInfo.LeafColumn column = file.column(field.column());
            if (column == null || column.flat() == false || column.repeated()) {
                continue;
            }
            final Conversion conversion = field.conversion() == Conversion.NONE ? encodingOf(column) : field.conversion();
            bindings.add(ParquetEngineFactory.bind(field.field(), number, column, conversion));
        }
        if (engineConfig.getIndexSettings().getMode().isColumnar()) {
            bindings.add(ColumnBinding.synthesizedId(fields.size()));
        }
        return bindings;
    }

    /** The conversion that a column needs whatever its field: only a floating point column needs one, its sortable encoding. */
    private static Conversion encodingOf(ParquetFileInfo.LeafColumn column) {
        return switch (column.physicalType()) {
            case FLOAT -> Conversion.SORTABLE_FLOAT;
            case DOUBLE -> Conversion.SORTABLE_DOUBLE;
            case INT32, INT64, BINARY, BOOLEAN, INT96, FIXED_LEN_BYTE_ARRAY -> Conversion.NONE;
        };
    }

    // ---- versions that the primary asks for ----

    /**
     * Completes the listener once the engine shows {@code minGeneration} or a later version of the table. The primary of an index of the
     * write engine gives the version of the table that holds its uploads as the generation of its refresh.
     */
    @Override
    public void addPrimaryTermAndGenerationListener(long minPrimaryTerm, long minGeneration, ActionListener<Long> listener) {
        final long loaded = loadedVersion;
        if (loaded >= minGeneration) {
            listener.onResponse(loaded);
            return;
        }
        synchronized (versionListeners) {
            versionListeners.add(Tuple.tuple(minGeneration, listener));
        }
        reloadExecutor.execute(this::readTableAndRefreshInBackground);
    }

    /**
     * Reads the table and refreshes the reader from a thread of the engine. The refresh does not block: a thread of the shard can hold
     * the engine lock of the shard while it refreshes, and the listeners of the refresh take that lock. If another refresh runs, it may
     * have started before the commit of the stubs, so the refresh is tried again a little later until the reader shows the commit.
     */
    private void readTableAndRefreshInBackground() {
        try {
            readTable();
            refreshInBackground();
        } catch (AlreadyClosedException e) {
            // the shard closed
        } catch (Exception e) {
            logger.warn(() -> "failed to read the table of " + shardId + ", the next refresh tries again", e);
            failVersionListeners(e);
        }
    }

    private void refreshInBackground() {
        try {
            ensureOpen();
            refreshReader(false);
            if (loadedVersion < committedVersion) {
                engineConfig.getThreadPool().schedule(this::refreshInBackground, TimeValue.timeValueMillis(100), reloadExecutor);
            }
        } catch (AlreadyClosedException e) {
            // the shard closed
        } catch (IOException e) {
            logger.warn(() -> "failed to refresh " + shardId + " after a read of its table", e);
            failVersionListeners(e);
        }
    }

    private void completeVersionListeners() {
        final long loaded = loadedVersion;
        final List<ActionListener<Long>> done = new ArrayList<>();
        synchronized (versionListeners) {
            versionListeners.removeIf(entry -> {
                if (entry.v1() <= loaded) {
                    done.add(entry.v2());
                    return true;
                }
                return false;
            });
        }
        for (ActionListener<Long> listener : done) {
            listener.onResponse(loaded);
        }
    }

    private void failVersionListeners(Exception e) {
        final List<ActionListener<Long>> failed;
        synchronized (versionListeners) {
            failed = new ArrayList<>();
            for (Tuple<Long, ActionListener<Long>> entry : versionListeners) {
                failed.add(entry.v2());
            }
            versionListeners.clear();
        }
        for (ActionListener<Long> listener : failed) {
            listener.onFailure(e);
        }
    }

    // ---- refresh: a read of the table ----

    /**
     * Reads the table for a refresh of the shard. A failure (the repository is missing or down, the table has delete files) does not fail
     * the refresh: the shard keeps serving the stubs that it has, the refreshes of the shard keep trying, and the listeners that wait for
     * a version keep waiting.
     */
    private void readTableOrWarn() {
        try {
            readTable();
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            logger.warn(() -> "failed to read the table of " + shardId + ", the shard serves the files that it read before", e);
        }
    }

    /** A refresh reads the table, and refreshes the reader of the shard. A thread of the shard calls it. */
    @Override
    public RefreshResult refresh(String source) throws EngineException {
        try {
            return onRepositoryThread(() -> {
                readTableOrWarn();
                final long before = loadedVersion;
                refreshReader(true);
                final boolean refreshed = loadedVersion != before;
                updateFieldInfos();
                return new RefreshResult(refreshed, config().getPrimaryTermSupplier().getAsLong(), loadedVersion);
            });
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            throw new RefreshFailedEngineException(shardId, e);
        }
    }

    /**
     * The scheduled refresh of the shard, every {@code index.refresh_interval}, reads the table: the refresh interval is how often a copy
     * looks for a new version. The search replicas of an index of the write engine also hear of each version that their primary commits
     * (see {@code TableVersionPublisher}), so for them this is only a fallback.
     */
    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        ActionListener.completeWith(listener, () -> onRepositoryThread(() -> {
            readTableOrWarn();
            final boolean refreshed = refreshReader(false);
            updateFieldInfos();
            return new RefreshResult(refreshed, config().getPrimaryTermSupplier().getAsLong(), loadedVersion);
        }));
    }

    /**
     * Refreshes the reader. The stubs that the refresh opens find their files through the resolver of the engine, which they take from
     * the context of the thread that opens them.
     *
     * @param blocking whether to wait for a refresh that runs already; one that does not wait returns {@code false} if one runs
     * @return whether this call refreshed
     */
    private boolean refreshReader(boolean blocking) throws IOException {
        return ParquetReadContext.with(access.resolver(), () -> {
            if (blocking) {
                readerManager.maybeRefreshBlocking();
                return true;
            }
            return readerManager.maybeRefresh();
        });
    }

    private void updateFieldInfos() throws IOException {
        final ElasticsearchDirectoryReader reader = readerManager.acquire();
        try {
            fieldInfos = FieldInfos.getMergedFieldInfos(reader);
        } finally {
            readerManager.release(reader);
        }
    }

    /** Only a read of the table tells whether it has a new version, so every scheduled refresh reads it. */
    @Override
    public boolean refreshNeeded() {
        return true;
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return readerManager;
    }

    // ---- reads ----

    /** A get looks the document up by its row number, which is its {@code _id}. */
    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        SplitShardCountSummary splitShardCountSummary,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        final Searcher searcher = acquireSearcher("get", SearcherScope.EXTERNAL, splitShardCountSummary, searcherWrapper);
        boolean found = false;
        try {
            final var docIdAndVersion = ParquetReadOnlyEngine.resolve(searcher.getIndexReader(), get.id());
            if (docIdAndVersion == null) {
                return GetResult.NOT_EXISTS;
            }
            found = true;
            return new GetResult(searcher, docIdAndVersion, false);
        } finally {
            if (found == false) {
                searcher.close();
            }
        }
    }

    @Override
    public GetResult getForUpdate(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        throw new UnsupportedOperationException("a copy that follows a Parquet table does not update documents");
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

    // ---- writes: none ----

    private UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException("[" + shardId + "] follows a Parquet table and is read-only");
    }

    @Override
    public IndexResult index(Index index) {
        throw readOnly();
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw readOnly();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw readOnly();
    }

    @Override
    public void writeIndexingBuffer() {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) throws EngineException {
        listener.onResponse(new FlushResult(false, getLastCommittedSegmentInfos().getGeneration()));
    }

    @Override
    public void flushAndClose() throws IOException {
        close();
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) {}

    // ---- sequence numbers and the translog: a copy that follows a table has neither ----

    @Override
    public long getMaxSeqNo() {
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED, globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return engineConfig.getGlobalCheckpointSupplier().getAsLong();
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
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
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return new TranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return Translog.Location.EMPTY;
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

    // ---- commits and stats ----

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        try {
            return store.readLastCommittedSegmentsInfo();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(SequenceNumbers.NO_OPS_PERFORMED, 0);
    }

    /** Nothing recovers from a copy that follows a table, so the last commit is handed out without protection. */
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
        final ElasticsearchDirectoryReader reader;
        try {
            reader = readerManager.acquire();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            return new DocsStats(reader.numDocs(), 0, 0);
        } finally {
            try {
                readerManager.release(reader);
            } catch (IOException e) {
                logger.debug("failed to release a reader", e);
            }
        }
    }

    @Override
    public ShardFieldStats shardFieldStats() {
        return new ShardFieldStats(0, 0, 0, 0, 0, 0);
    }

    /** The fields of the stubs: the fields that the files have values for, which field capabilities and ES|QL ask for. */
    @Override
    public FieldInfos shardFieldInfos() {
        return fieldInfos;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats();
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
                failVersionListeners(new AlreadyClosedException("[" + shardId + "] is closed: " + reason));
                IOUtils.close(readerManager, store::decRef);
            } catch (Exception e) {
                logger.warn("failed to close the engine that follows a Parquet table", e);
            } finally {
                closedLatch.countDown();
            }
        }
    }

}
