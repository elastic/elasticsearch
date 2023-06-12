/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.translog.TranslogConfig.EMPTY_TRANSLOG_BUFFER_SIZE;

/**
 * A Translog is a per index shard component that records all non-committed index operations in a durable manner.
 * In Elasticsearch there is one Translog instance per {@link org.elasticsearch.index.engine.InternalEngine}.
 * Additionally, since Elasticsearch 2.0 the engine also records a {@link #TRANSLOG_UUID_KEY} with each commit to ensure a strong
 * association between the lucene index an the transaction log file. This UUID is used to prevent accidental recovery from a transaction
 * log that belongs to a
 * different engine.
 * <p>
 * Each Translog has only one translog file open for writes at any time referenced by a translog generation ID. This ID is written to a
 * {@code translog.ckp} file that is designed to fit in a single disk block such that a write of the file is atomic. The checkpoint file
 * is written on each fsync operation of the translog and records the number of operations written, the current translog's file generation,
 * its fsynced offset in bytes, and other important statistics.
 * </p>
 * <p>
 * When the current translog file reaches a certain size ({@link IndexSettings#INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING}, or when
 * a clear separation between old and new operations (upon change in primary term), the current file is reopened for read only and a new
 * write only file is created. Any non-current, read only translog file always has a {@code translog-${gen}.ckp} associated with it
 * which is an fsynced copy of its last {@code translog.ckp} such that in disaster recovery last fsynced offsets, number of
 * operation etc. are still preserved.
 * </p>
 */
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable {

    /*
     * TODO
     *  - we might need something like a deletion policy to hold on to more than one translog eventually (I think sequence IDs needs this)
     *    but we can refactor as we go
     *  - use a simple BufferedOutputStream to write stuff and fold BufferedTranslogWriter into it's super class... the tricky bit is we
     *    need to be able to do random access reads even from the buffer
     *  - we need random exception on the FileSystem API tests for all this.
     *  - we need to page align the last write before we sync, we can take advantage of ensureSynced for this since we might have already
     *    fsynced far enough
     */
    public static final String TRANSLOG_UUID_KEY = "translog_uuid";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";
    public static final String CHECKPOINT_SUFFIX = ".ckp";
    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");
    public static final int DEFAULT_HEADER_SIZE_IN_BYTES = TranslogHeader.headerSizeInBytes(UUIDs.randomBase64UUID());

    // the list of translog readers is guaranteed to be in order of translog generation
    private final List<TranslogReader> readers = new ArrayList<>();
    private final BigArrays bigArrays;
    private final DiskIoBufferPool diskIoBufferPool;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    private final Path location;
    private TranslogWriter current;

    protected final TragicExceptionHolder tragedy = new TragicExceptionHolder();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final TranslogConfig config;
    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier primaryTermSupplier;
    private final String translogUUID;

    private final boolean useFsync;

    private final TranslogDeletionPolicy deletionPolicy;
    private final LongConsumer persistedSequenceNumberConsumer;
    private final OperationListener operationListener;

    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                   the configuration of this translog
     * @param translogUUID             the translog uuid to open, null for a new translog
     * @param deletionPolicy           an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                 deleted
     * @param globalCheckpointSupplier a supplier for the global checkpoint
     * @param primaryTermSupplier      a supplier for the latest value of primary term of the owning index shard. The latest term value is
     *                                 examined and stored in the header whenever a new generation is rolled. It's guaranteed from outside
     *                                 that a new generation is rolled when the term is increased. This guarantee allows to us to validate
     *                                 and reject operation whose term is higher than the primary term stored in the translog header.
     * @param persistedSequenceNumberConsumer a callback that's called whenever an operation with a given sequence number is successfully
     *                                        persisted.
     */
    public Translog(
        final TranslogConfig config,
        final String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier primaryTermSupplier,
        final LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {
        super(config.getShardId(), config.getIndexSettings());
        this.config = config;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.primaryTermSupplier = primaryTermSupplier;
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.operationListener = config.getOperationListener();
        this.deletionPolicy = deletionPolicy;
        this.translogUUID = translogUUID;
        this.useFsync = IndexModule.NODE_STORE_USE_FSYNC.get(config.getIndexSettings().getNodeSettings());
        bigArrays = config.getBigArrays();
        diskIoBufferPool = config.getDiskIoBufferPool();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();
        Files.createDirectories(this.location);

        try {
            final Checkpoint checkpoint = readCheckpoint(location);
            final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
            final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            // this is special handling for error condition when we create a new writer but we fail to bake
            // the newly written file (generation+1) into the checkpoint. This is still a valid state
            // we just need to cleanup before we continue
            // we hit this before and then blindly deleted the new generation even though we managed to bake it in and then hit this:
            // https://discuss.elastic.co/t/cannot-recover-index-because-of-missing-tanslog-files/38336 as an example
            //
            // For this to happen we must have already copied the translog.ckp file into translog-gen.ckp so we first check if that
            // file exists. If not we don't even try to clean it up and wait until we fail creating it
            assert Files.exists(nextTranslogFile) == false || Files.size(nextTranslogFile) <= TranslogHeader.headerSizeInBytes(translogUUID)
                : "unexpected translog file: [" + nextTranslogFile + "]";
            if (Files.exists(currentCheckpointFile) // current checkpoint is already copied
                && Files.deleteIfExists(nextTranslogFile)) { // delete it and log a warning
                logger.warn(
                    "deleted previously created, but not yet committed, next generation [{}]. This can happen due to a"
                        + " tragic exception when creating a new generation",
                    nextTranslogFile.getFileName()
                );
            }
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(
                    checkpoint.generation + 1,
                    getMinFileGeneration(),
                    checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer
                );
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    /** recover all translog files found on disk */
    private ArrayList<TranslogReader> recoverFromFiles(Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock ignored = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);

            final long minGenerationToRecoverFrom = checkpoint.minTranslogGeneration;
            assert minGenerationToRecoverFrom >= 0 : "minTranslogGeneration should be non-negative";

            // we open files in reverse order in order to validate the translog uuid before we start traversing the translog based on
            // the generation id we found in the lucene commit. This gives for better error messages if the wrong
            // translog was found.
            for (long i = checkpoint.generation; i >= minGenerationToRecoverFrom; i--) {
                Path committedTranslogFile = location.resolve(getFilename(i));
                final Checkpoint readerCheckpoint = i == checkpoint.generation
                    ? checkpoint
                    : Checkpoint.read(location.resolve(getCommitCheckpointFileName(i)));
                final TranslogReader reader;
                try {
                    reader = openReader(committedTranslogFile, readerCheckpoint);
                } catch (NoSuchFileException fnfe) {
                    throw new TranslogCorruptedException(
                        committedTranslogFile.toString(),
                        "translog file doesn't exist with generation: "
                            + i
                            + " recovering from: "
                            + minGenerationToRecoverFrom
                            + " checkpoint: "
                            + checkpoint.generation
                            + " - translog ids must be consecutive"
                    );
                }
                assert reader.getPrimaryTerm() <= primaryTermSupplier.getAsLong()
                    : "Primary terms go backwards; current term ["
                        + primaryTermSupplier.getAsLong()
                        + "] translog path [ "
                        + committedTranslogFile
                        + ", existing term ["
                        + reader.getPrimaryTerm()
                        + "]";
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            Collections.reverse(foundTranslogs);

            // when we clean up files, we first update the checkpoint with a new minReferencedTranslog and then delete them;
            // if we crash just at the wrong moment, it may be that we leave one unreferenced file behind so we delete it if there
            IOUtils.deleteFilesIgnoringExceptions(
                location.resolve(getFilename(minGenerationToRecoverFrom - 1)),
                location.resolve(getCommitCheckpointFileName(minGenerationToRecoverFrom - 1))
            );

            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                Checkpoint checkpointFromDisk = Checkpoint.read(commitCheckpoint);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new TranslogCorruptedException(
                        commitCheckpoint.toString(),
                        "checkpoint file "
                            + commitCheckpoint.getFileName()
                            + " already exists but has corrupted content: expected "
                            + checkpoint
                            + " but got "
                            + checkpointFromDisk
                    );
                }
            } else {
                copyCheckpointTo(commitCheckpoint);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
        }
        return foundTranslogs;
    }

    private void copyCheckpointTo(Path targetPath) throws IOException {
        // a temp file to copy checkpoint to - note it must be in on the same FS otherwise atomic move won't work
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, CHECKPOINT_SUFFIX);
        boolean tempFileRenamed = false;

        try {
            // we first copy this into the temp-file and then fsync it followed by an atomic move into the target file
            // that way if we hit a disk-full here we are still in an consistent state.
            Files.copy(location.resolve(CHECKPOINT_FILE_NAME), tempFile, StandardCopyOption.REPLACE_EXISTING);
            if (useFsync) {
                IOUtils.fsync(tempFile, false);
            }
            Files.move(tempFile, targetPath, StandardCopyOption.ATOMIC_MOVE);
            tempFileRenamed = true;
            // we only fsync the directory the tempFile was already fsynced
            if (useFsync) {
                IOUtils.fsync(targetPath.getParent(), true);
            }
        } finally {
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    logger.warn(() -> format("failed to delete temp file %s", tempFile), ex);
                }
            }
        }
    }

    TranslogReader openReader(Path path, Checkpoint checkpoint) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            assert Translog.parseIdFromFileName(path) == checkpoint.generation
                : "expected generation: " + Translog.parseIdFromFileName(path) + " but got: " + checkpoint.generation;
            TranslogReader reader = TranslogReader.open(channel, path, checkpoint, translogUUID);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }

    /**
     * Extracts the translog generation from a file name.
     *
     * @throws IllegalArgumentException if the path doesn't match the expected pattern.
     */
    public static long parseIdFromFileName(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_STRICT_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                    "number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " + fileName + "]",
                    e
                );
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }

    /** Returns {@code true} if this {@code Translog} is still open. */
    public boolean isOpen() {
        return closed.get() == false;
    }

    private static boolean calledFromOutsideOrViaTragedyClose() {
        List<StackTraceElement> frames = Stream.of(Thread.currentThread().getStackTrace()).skip(3). // skip getStackTrace, current method
                                                                                                    // and close method frames
            limit(10). // limit depth of analysis to 10 frames, it should be enough to catch closing with, e.g. IOUtils
            filter(f -> {
                try {
                    return Translog.class.isAssignableFrom(Class.forName(f.getClassName()));
                } catch (Exception ignored) {
                    return false;
                }
            }). // find all inner callers including Translog subclasses
            collect(Collectors.toList());
        // the list of inner callers should be either empty or should contain closeOnTragicEvent method
        return frames.isEmpty() || frames.stream().anyMatch(f -> f.getMethodName().equals("closeOnTragicEvent"));
    }

    @Override
    public void close() throws IOException {
        assert calledFromOutsideOrViaTragedyClose()
            : "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    current.sync();
                } finally {
                    closeFilesIfNoPendingRetentionLocks();
                }
            } finally {
                logger.debug("translog closed");
            }
        }
    }

    /**
     * Returns all translog locations as absolute paths.
     * These paths don't contain actual translog files they are
     * directories holding the transaction logs.
     */
    public Path location() {
        return location;
    }

    /**
     * Returns the generation of the current transaction log.
     */
    public long currentFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    /**
     * Returns the minimum file generation referenced by the translog
     */
    public long getMinFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            if (readers.isEmpty()) {
                return current.getGeneration();
            } else {
                assert readers.stream().map(TranslogReader::getGeneration).min(Long::compareTo).get().equals(readers.get(0).getGeneration())
                    : "the first translog isn't the one with the minimum generation:" + readers;
                return readers.get(0).getGeneration();
            }
        }
    }

    /**
     * Returns the number of operations in the translog files
     */
    public int totalOperations() {
        return totalOperationsByMinGen(-1);
    }

    /**
     * Returns the size in bytes of the v files
     */
    public long sizeInBytes() {
        return sizeInBytesByMinGen(-1);
    }

    long earliestLastModifiedAge() {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return findEarliestLastModifiedAge(System.currentTimeMillis(), readers, current);
        } catch (IOException e) {
            throw new TranslogException(shardId, "Unable to get the earliest last modified time for the transaction log");
        }
    }

    /**
     * Returns the age of the oldest entry in the translog files in seconds
     */
    static long findEarliestLastModifiedAge(long currentTime, Iterable<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long earliestTime = currentTime;
        for (TranslogReader r : readers) {
            earliestTime = Math.min(r.getLastModifiedTime(), earliestTime);
        }
        return Math.max(0, currentTime - Math.min(earliestTime, writer.getLastModifiedTime()));
    }

    /**
     * Returns the number of operations in the translog files at least the given generation
     */
    public int totalOperationsByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current))
                .filter(r -> r.getGeneration() >= minGeneration)
                .mapToInt(BaseTranslogReader::totalOperations)
                .sum();
        }
    }

    /**
     * Returns the number of operations in the transaction files that contain operations with seq# above the given number.
     */
    public int estimateTotalOperationsFromMinSeq(long minSeqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return readersAboveMinSeqNo(minSeqNo).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    /**
     * Returns the size in bytes of the translog files at least the given generation
     */
    public long sizeInBytesByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current))
                .filter(r -> r.getGeneration() >= minGeneration)
                .mapToLong(BaseTranslogReader::sizeInBytes)
                .sum();
        }
    }

    /**
     * Creates a new translog for the specified generation.
     *
     * @param fileGeneration the translog generation
     * @return a writer for the new translog
     * @throws IOException if creating the translog failed
     */
    TranslogWriter createWriter(long fileGeneration) throws IOException {
        final TranslogWriter writer = createWriter(
            fileGeneration,
            getMinFileGeneration(),
            globalCheckpointSupplier.getAsLong(),
            persistedSequenceNumberConsumer
        );
        assert writer.sizeInBytes() == DEFAULT_HEADER_SIZE_IN_BYTES
            : "Mismatch translog header size; "
                + "empty translog size ["
                + writer.sizeInBytes()
                + ", header size ["
                + DEFAULT_HEADER_SIZE_IN_BYTES
                + "]";
        return writer;
    }

    /**
     * creates a new writer
     *
     * @param fileGeneration          the generation of the write to be written
     * @param initialMinTranslogGen   the minimum translog generation to be written in the first checkpoint. This is
     *                                needed to solve and initialization problem while constructing an empty translog.
     *                                With no readers and no current, a call to  {@link #getMinFileGeneration()} would not work.
     * @param initialGlobalCheckpoint the global checkpoint to be written in the first checkpoint.
     */
    TranslogWriter createWriter(
        long fileGeneration,
        long initialMinTranslogGen,
        long initialGlobalCheckpoint,
        LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {
        final TranslogWriter newWriter;
        try {
            newWriter = TranslogWriter.create(
                shardId,
                translogUUID,
                fileGeneration,
                location.resolve(getFilename(fileGeneration)),
                getChannelFactory(),
                useFsync,
                config.getBufferSize(),
                initialMinTranslogGen,
                initialGlobalCheckpoint,
                globalCheckpointSupplier,
                this::getMinFileGeneration,
                primaryTermSupplier.getAsLong(),
                tragedy,
                persistedSequenceNumberConsumer,
                bigArrays,
                diskIoBufferPool,
                operationListener
            );
        } catch (final IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newWriter;
    }

    /**
     * Adds an operation to the transaction log.
     *
     * @param operation the operation to add
     * @return the location of the operation in the translog
     * @throws IOException if adding the operation to the translog resulted in an I/O exception
     */
    public Location add(final Operation operation) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            writeOperationWithSize(out, operation);
            final BytesReference bytes = out.bytes();
            try (ReleasableLock ignored = readLock.acquire()) {
                ensureOpen();
                if (operation.primaryTerm() > current.getPrimaryTerm()) {
                    assert false
                        : "Operation term is newer than the current term; "
                            + "current term["
                            + current.getPrimaryTerm()
                            + "], operation term["
                            + operation
                            + "]";
                    throw new IllegalArgumentException(
                        "Operation term is newer than the current term; "
                            + "current term["
                            + current.getPrimaryTerm()
                            + "], operation term["
                            + operation
                            + "]"
                    );
                }
                return current.add(bytes, operation.seqNo());
            }
        } catch (final AlreadyClosedException | IOException ex) {
            closeOnTragicEvent(ex);
            throw ex;
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", ex);
        } finally {
            Releasables.close(out);
        }
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test
     * is based on the size of the current generation compared to the configured generation
     * threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public boolean shouldRollGeneration() {
        final long threshold = this.indexSettings.getGenerationThresholdSize().getBytes();
        try (ReleasableLock ignored = readLock.acquire()) {
            return this.current.sizeInBytes() > threshold;
        }
    }

    /**
     * The a {@linkplain Location} that will sort after the {@linkplain Location} returned by the last write but before any locations which
     * can be returned by the next write.
     */
    public Location getLastWriteLocation() {
        try (ReleasableLock lock = readLock.acquire()) {
            /*
             * We use position = current - 1 and size = Integer.MAX_VALUE here instead of position current and size = 0 for two reasons:
             * 1. Translog.Location's compareTo doesn't actually pay attention to size even though it's equals method does.
             * 2. It feels more right to return a *position* that is before the next write's position rather than rely on the size.
             */
            return new Location(current.generation, current.sizeInBytes() - 1, Integer.MAX_VALUE);
        }
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getLastSyncedCheckpoint().globalCheckpoint;
    }

    final Checkpoint getLastSyncedCheckpoint() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getLastSyncedCheckpoint();
        }
    }

    // for testing
    public Snapshot newSnapshot() throws IOException {
        return newSnapshot(0, Long.MAX_VALUE);
    }

    /**
     * Creates a new translog snapshot containing operations from the given range.
     *
     * @param fromSeqNo the lower bound of the range (inclusive)
     * @param toSeqNo   the upper bound of the range (inclusive)
     * @return the new snapshot
     */
    public Snapshot newSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "from_seq_no must be non-negative " + fromSeqNo;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            TranslogSnapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current))
                .filter(reader -> reader.getCheckpoint().minSeqNo <= toSeqNo && fromSeqNo <= reader.getCheckpoint().maxEffectiveSeqNo())
                .map(BaseTranslogReader::newSnapshot)
                .toArray(TranslogSnapshot[]::new);
            final Snapshot snapshot = newMultiSnapshot(snapshots);
            return new SeqNoFilterSnapshot(snapshot, fromSeqNo, toSeqNo);
        }
    }

    /**
     * Reads and returns the operation from the given location if the generation it references is still available. Otherwise
     * this method will return <code>null</code>.
     */
    public Operation readOperation(Location location) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (location.generation < getMinFileGeneration()) {
                return null;
            }
            if (current.generation == location.generation) {
                // no need to fsync here the read operation will ensure that buffers are written to disk
                // if they are still in RAM and we are reading onto that position
                return current.read(location);
            } else {
                // read backwards - it's likely we need to read on that is recent
                for (int i = readers.size() - 1; i >= 0; i--) {
                    TranslogReader translogReader = readers.get(i);
                    if (translogReader.generation == location.generation) {
                        return translogReader.read(location);
                    }
                }
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return null;
    }

    private Snapshot newMultiSnapshot(TranslogSnapshot[] snapshots) throws IOException {
        final Closeable onClose;
        if (snapshots.length == 0) {
            onClose = () -> {};
        } else {
            assert Arrays.stream(snapshots).map(BaseTranslogReader::getGeneration).min(Long::compareTo).get() == snapshots[0].generation
                : "first reader generation of " + snapshots + " is not the smallest";
            onClose = acquireTranslogGenFromDeletionPolicy(snapshots[0].generation);
        }
        boolean success = false;
        try {
            Snapshot result = new MultiSnapshot(snapshots, onClose);
            success = true;
            return result;
        } finally {
            if (success == false) {
                onClose.close();
            }
        }
    }

    private Stream<? extends BaseTranslogReader> readersAboveMinSeqNo(long minSeqNo) {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread()
            : "callers of readersAboveMinSeqNo must hold a lock: readLock ["
                + readLock.isHeldByCurrentThread()
                + "], writeLock ["
                + readLock.isHeldByCurrentThread()
                + "]";
        return Stream.concat(readers.stream(), Stream.of(current)).filter(reader -> minSeqNo <= reader.getCheckpoint().maxEffectiveSeqNo());
    }

    /**
     * Acquires a lock on the translog files, preventing them from being trimmed
     */
    public Closeable acquireRetentionLock() {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final long viewGen = getMinFileGeneration();
            return acquireTranslogGenFromDeletionPolicy(viewGen);
        }
    }

    private Closeable acquireTranslogGenFromDeletionPolicy(long viewGen) {
        Releasable toClose = deletionPolicy.acquireTranslogGen(viewGen);
        return () -> {
            try {
                toClose.close();
            } finally {
                trimUnreferencedReaders();
                closeFilesIfNoPendingRetentionLocks();
            }
        };
    }

    /**
     * Sync's the translog.
     */
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    /**
     *  Returns <code>true</code> if an fsync is required to ensure durability of the translogs operations or it's metadata.
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    /** package private for testing */
    public static String getFilename(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + TRANSLOG_FILE_SUFFIX;
    }

    static String getCommitCheckpointFileName(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + CHECKPOINT_SUFFIX;
    }

    /**
     * Trims translog for terms of files below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>.
     * Effectively it moves max visible seq# {@link Checkpoint#trimmedAboveSeqNo} therefore {@link TranslogSnapshot} skips those operations.
     */
    public void trimOperations(long belowTerm, long aboveSeqNo) throws IOException {
        assert aboveSeqNo >= SequenceNumbers.NO_OPS_PERFORMED : "aboveSeqNo has to a valid sequence number";

        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            if (current.getPrimaryTerm() < belowTerm) {
                throw new IllegalArgumentException(
                    "Trimming the translog can only be done for terms lower than the current one. "
                        + "Trim requested for term [ "
                        + belowTerm
                        + " ] , current is [ "
                        + current.getPrimaryTerm()
                        + " ]"
                );
            }
            // we assume that the current translog generation doesn't have trimmable ops. Verify that.
            assert current.assertNoSeqAbove(belowTerm, aboveSeqNo);
            // update all existed ones (if it is necessary) as checkpoint and reader are immutable
            final List<TranslogReader> newReaders = new ArrayList<>(readers.size());
            try {
                for (TranslogReader reader : readers) {
                    final TranslogReader newReader = reader.getPrimaryTerm() < belowTerm
                        ? reader.closeIntoTrimmedReader(aboveSeqNo, getChannelFactory(), useFsync)
                        : reader;
                    newReaders.add(newReader);
                }
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(newReaders);
                tragedy.setTragicException(e);
                closeOnTragicEvent(e);
                throw e;
            }

            this.readers.clear();
            this.readers.addAll(newReaders);
        }
    }

    /**
     * Ensures that the given location and global checkpoint has be synced / written to the underlying storage.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Location location, long globalCheckpoint) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            // if we have a new generation and the persisted global checkpoint is greater than or equal to the sync global checkpoint it's
            // already synced
            long persistedGlobalCheckpoint = current.getLastSyncedCheckpoint().globalCheckpoint;
            if (location.generation == current.getGeneration() || persistedGlobalCheckpoint < globalCheckpoint) {
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size, globalCheckpoint);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    /**
     * Closes the translog if the current translog writer experienced a tragic exception.
     *
     * Note that in case this thread closes the translog it must not already be holding a read lock on the translog as it will acquire a
     * write lock in the course of closing the translog
     *
     * @param ex if an exception occurs closing the translog, it will be suppressed into the provided exception
     */
    protected void closeOnTragicEvent(final Exception ex) {
        // we can not hold a read lock here because closing will attempt to obtain a write lock and that would result in self-deadlock
        assert readLock.isHeldByCurrentThread() == false : Thread.currentThread().getName();
        if (tragedy.get() != null) {
            try {
                close();
            } catch (final AlreadyClosedException inner) {
                /*
                 * Don't do anything in this case. The AlreadyClosedException comes from TranslogWriter and we should not add it as
                 * suppressed because it will contain the provided exception as its cause. See also
                 * https://github.com/elastic/elasticsearch/issues/15941.
                 */
            } catch (final Exception inner) {
                assert ex != inner.getCause();
                ex.addSuppressed(inner);
            }
        }
    }

    /**
     * return stats
     */
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            final long uncommittedGen = minGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1, current, readers);
            return new TranslogStats(
                totalOperations(),
                sizeInBytes(),
                totalOperationsByMinGen(uncommittedGen),
                sizeInBytesByMinGen(uncommittedGen),
                earliestLastModifiedAge()
            );
        }
    }

    public TranslogConfig getConfig() {
        return config;
    }

    // public for testing
    public TranslogDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }

    public static class Location implements Comparable<Location> {

        public static Location EMPTY = new Location(0, 0, 0);

        public final long generation;
        public final long translogLocation;
        public final int size;

        public Location(long generation, long translogLocation, int size) {
            this.generation = generation;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        @Override
        public String toString() {
            return "[generation: " + generation + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (generation == o.generation) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(generation, o.generation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Location location = (Location) o;

            if (generation != location.generation) {
                return false;
            }
            if (translogLocation != location.translogLocation) {
                return false;
            }
            return size == location.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(generation);
            result = 31 * result + Long.hashCode(translogLocation);
            result = 31 * result + size;
            return result;
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    public interface Snapshot extends Closeable {

        Snapshot EMPTY = new Snapshot() {
            @Override
            public void close() {}

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Operation next() {
                return null;
            }
        };

        /**
         * The total estimated number of operations in the snapshot.
         */
        int totalOperations();

        /**
         * The number of operations have been skipped (overridden or trimmed) in the snapshot so far.
         * Unlike {@link #totalOperations()}, this value is updated each time after {@link #next()}) is called.
         */
        default int skippedOperations() {
            return 0;
        }

        /**
         * Returns the next operation in the snapshot or <code>null</code> if we reached the end.
         */
        Translog.Operation next() throws IOException;
    }

    /**
     * A filtered snapshot consisting of only operations whose sequence numbers are in the given range
     * between {@code fromSeqNo} (inclusive) and {@code toSeqNo} (inclusive). This filtered snapshot
     * shares the same underlying resources with the {@code delegate} snapshot, therefore we should not
     * use the {@code delegate} after passing it to this filtered snapshot.
     */
    private static final class SeqNoFilterSnapshot implements Snapshot {
        private final Snapshot delegate;
        private int filteredOpsCount;
        private final long fromSeqNo; // inclusive
        private final long toSeqNo;   // inclusive

        SeqNoFilterSnapshot(Snapshot delegate, long fromSeqNo, long toSeqNo) {
            assert fromSeqNo <= toSeqNo : "from_seq_no[" + fromSeqNo + "] > to_seq_no[" + toSeqNo + "]";
            this.delegate = delegate;
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
        }

        @Override
        public int totalOperations() {
            return delegate.totalOperations();
        }

        @Override
        public int skippedOperations() {
            return filteredOpsCount + delegate.skippedOperations();
        }

        @Override
        public Operation next() throws IOException {
            Translog.Operation op;
            while ((op = delegate.next()) != null) {
                if (fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo) {
                    return op;
                } else {
                    filteredOpsCount++;
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    public abstract static sealed class Operation implements Writeable permits Delete, Index, NoOp {
        public enum Type {
            @Deprecated
            CREATE((byte) 1),
            INDEX((byte) 2),
            DELETE((byte) 3),
            NO_OP((byte) 4);

            private final byte id;

            Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                return switch (id) {
                    case 1 -> CREATE;
                    case 2 -> INDEX;
                    case 3 -> DELETE;
                    case 4 -> NO_OP;
                    default -> throw new IllegalArgumentException("no type mapped for [" + id + "]");
                };
            }
        }

        protected final long seqNo;

        protected final long primaryTerm;

        protected Operation(long seqNo, long primaryTerm) {
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }

        public abstract Type opType();

        public abstract long estimateSize();

        public final long seqNo() {
            return seqNo;
        }

        public final long primaryTerm() {
            return primaryTerm;
        }

        /**
         * Reads the type and the operation from the given stream.
         */
        public static Operation readOperation(final StreamInput input) throws IOException {
            final Translog.Operation.Type type = Translog.Operation.Type.fromId(input.readByte());
            return switch (type) {
                // the de-serialization logic in Index was identical to that of Create when create was deprecated
                case CREATE, INDEX -> Index.readFrom(input);
                case DELETE -> Delete.readFrom(input);
                case NO_OP -> new NoOp(input);
            };
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeByte(opType().id());
            writeBody(out);
        }

        protected abstract void writeBody(StreamOutput out) throws IOException;
    }

    public static final class Index extends Operation {

        public static final int FORMAT_NO_PARENT = 9; // since 7.0
        public static final int FORMAT_NO_VERSION_TYPE = FORMAT_NO_PARENT + 1;
        public static final int FORMAT_NO_DOC_TYPE = FORMAT_NO_VERSION_TYPE + 1;
        public static final int SERIALIZATION_FORMAT = FORMAT_NO_DOC_TYPE;

        private final String id;
        private final long autoGeneratedIdTimestamp;
        private final long version;
        private final BytesReference source;
        private final String routing;

        private static Index readFrom(StreamInput in) throws IOException {
            final int format = in.readVInt(); // SERIALIZATION_FORMAT
            assert format >= FORMAT_NO_PARENT : "format was: " + format;
            String id = in.readString();
            if (format < FORMAT_NO_DOC_TYPE) {
                in.readString();
                // can't assert that this is _doc because pre-8.0 indexes can have any name for a type
            }
            BytesReference source = in.readBytesReference();
            String routing = in.readOptionalString();
            long version = in.readLong();
            if (format < FORMAT_NO_VERSION_TYPE) {
                in.readByte(); // _version_type
            }
            long autoGeneratedIdTimestamp = in.readLong();
            long seqNo = in.readLong();
            long primaryTerm = in.readLong();
            return new Index(id, seqNo, primaryTerm, version, source, routing, autoGeneratedIdTimestamp);
        }

        public Index(Engine.Index index, Engine.IndexResult indexResult) {
            this(
                index.id(),
                indexResult.getSeqNo(),
                index.primaryTerm(),
                indexResult.getVersion(),
                index.source(),
                index.routing(),
                index.getAutoGeneratedIdTimestamp()
            );
        }

        public Index(
            String id,
            long seqNo,
            long primaryTerm,
            long version,
            BytesReference source,
            String routing,
            long autoGeneratedIdTimestamp
        ) {
            super(seqNo, primaryTerm);
            this.id = id;
            this.source = source;
            this.version = version;
            this.routing = routing;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public long estimateSize() {
            return (2 * id.length()) + source.length() + (routing != null ? 2 * routing.length() : 0) + (4 * Long.BYTES); // timestamp,
                                                                                                                          // seq_no,
                                                                                                                          // primary_term,
                                                                                                                          // and version
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public BytesReference source() {
            return this.source;
        }

        public long version() {
            return this.version;
        }

        @Override
        public void writeBody(final StreamOutput out) throws IOException {
            final int format = out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)
                ? SERIALIZATION_FORMAT
                : FORMAT_NO_VERSION_TYPE;
            out.writeVInt(format);
            out.writeString(id);
            if (format < FORMAT_NO_DOC_TYPE) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeBytesReference(source);
            out.writeOptionalString(routing);
            out.writeLong(version);
            out.writeLong(autoGeneratedIdTimestamp);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Index index = (Index) o;

            if (version != index.version
                || seqNo != index.seqNo
                || primaryTerm != index.primaryTerm
                || id.equals(index.id) == false
                || autoGeneratedIdTimestamp != index.autoGeneratedIdTimestamp
                || source.equals(index.source) == false) {
                return false;
            }
            return Objects.equals(routing, index.routing);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + Long.hashCode(autoGeneratedIdTimestamp);
            return result;
        }

        @Override
        public String toString() {
            return "Index{"
                + "id='"
                + id
                + '\''
                + ", seqNo="
                + seqNo
                + ", primaryTerm="
                + primaryTerm
                + ", version="
                + version
                + ", autoGeneratedIdTimestamp="
                + autoGeneratedIdTimestamp
                + '}';
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

    }

    public static final class Delete extends Operation {

        private static final int FORMAT_6_0 = 4; // 6.0 - *
        public static final int FORMAT_NO_PARENT = FORMAT_6_0 + 1; // since 7.0
        public static final int FORMAT_NO_VERSION_TYPE = FORMAT_NO_PARENT + 1;
        public static final int FORMAT_NO_DOC_TYPE = FORMAT_NO_VERSION_TYPE + 1;    // since 8.0
        public static final int SERIALIZATION_FORMAT = FORMAT_NO_DOC_TYPE;

        private final String id;
        private final long version;

        private static Delete readFrom(StreamInput in) throws IOException {
            final int format = in.readVInt();// SERIALIZATION_FORMAT
            assert format >= FORMAT_6_0 : "format was: " + format;
            if (format < FORMAT_NO_DOC_TYPE) {
                in.readString();
                // Can't assert that this is _doc because pre-8.0 indexes can have any name for a type
            }
            String id = in.readString();
            if (format < FORMAT_NO_DOC_TYPE) {
                final String docType = in.readString();
                assert docType.equals(IdFieldMapper.NAME) : docType + " != " + IdFieldMapper.NAME;
                in.readBytesRef(); // uid
            }
            long version = in.readLong();
            if (format < FORMAT_NO_VERSION_TYPE) {
                in.readByte(); // versionType
            }
            long seqNo = in.readLong();
            long primaryTerm = in.readLong();
            return new Delete(id, seqNo, primaryTerm, version);
        }

        public Delete(Engine.Delete delete, Engine.DeleteResult deleteResult) {
            this(delete.id(), deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getVersion());
        }

        /** utility for testing */
        public Delete(String id, long seqNo, long primaryTerm) {
            this(id, seqNo, primaryTerm, Versions.MATCH_ANY);
        }

        public Delete(String id, long seqNo, long primaryTerm, long version) {
            super(seqNo, primaryTerm);
            this.id = Objects.requireNonNull(id);
            this.version = version;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return (2 * id.length()) + (3 * Long.BYTES); // seq_no, primary_term, and version;
        }

        public String id() {
            return id;
        }

        public long version() {
            return this.version;
        }

        @Override
        public void writeBody(final StreamOutput out) throws IOException {
            final int format = out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)
                ? SERIALIZATION_FORMAT
                : FORMAT_NO_VERSION_TYPE;
            out.writeVInt(format);
            if (format < FORMAT_NO_DOC_TYPE) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            if (format < FORMAT_NO_DOC_TYPE) {
                out.writeString(IdFieldMapper.NAME);
                out.writeBytesRef(Uid.encodeId(id));
            }
            out.writeLong(version);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Delete delete = (Delete) o;

            return id.equals(delete.id) && seqNo == delete.seqNo && primaryTerm == delete.primaryTerm && version == delete.version;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result += 31 * Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" + "id='" + id + "', seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", version=" + version + '}';
        }
    }

    public static final class NoOp extends Operation {
        private final String reason;

        public String reason() {
            return reason;
        }

        private NoOp(final StreamInput in) throws IOException {
            this(in.readLong(), in.readLong(), in.readString());
        }

        public NoOp(final long seqNo, final long primaryTerm, final String reason) {
            super(seqNo, primaryTerm);
            assert seqNo > SequenceNumbers.NO_OPS_PERFORMED;
            assert primaryTerm >= 0;
            assert reason != null;
            this.reason = reason;
        }

        @Override
        public void writeBody(final StreamOutput out) throws IOException {
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
            out.writeString(reason);
        }

        @Override
        public Type opType() {
            return Type.NO_OP;
        }

        @Override
        public long estimateSize() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NoOp that = (NoOp) obj;
            return seqNo == that.seqNo && primaryTerm == that.primaryTerm && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return 31 * 31 * Long.hashCode(seqNo) + 31 * Long.hashCode(primaryTerm) + reason().hashCode();
        }

        @Override
        public String toString() {
            return "NoOp{" + "seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", reason='" + reason + '\'' + '}';
        }
    }

    public enum Durability {

        /**
         * Async durability - translogs are synced based on a time interval.
         */
        ASYNC,
        /**
         * Request durability - translogs are synced for each high level request (bulk, index, delete)
         */
        REQUEST

    }

    static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = Integer.toUnsignedLong(in.readInt());
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException(
                in.getSource(),
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
    }

    /**
     * Reads a list of operations written with {@link #writeOperations(StreamOutput, List)}
     */
    public static List<Operation> readOperations(StreamInput input, String source) throws IOException {
        ArrayList<Operation> operations = new ArrayList<>();
        int numOps = input.readInt();
        final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(input, source);
        if (input.getTransportVersion().before(TransportVersion.V_8_8_0)) {
            for (int i = 0; i < numOps; i++) {
                operations.add(readOperation(checksumStreamInput));
            }
        } else {
            for (int i = 0; i < numOps; i++) {
                checksumStreamInput.resetDigest();
                operations.add(Translog.Operation.readOperation(checksumStreamInput));
                verifyChecksum(checksumStreamInput);
            }
        }
        return operations;
    }

    public static Translog.Operation readOperation(BufferedChecksumStreamInput in) throws IOException {
        final Translog.Operation operation;
        try {
            final int opSize = in.readInt();
            if (opSize < 4) { // 4byte for the checksum
                throw new TranslogCorruptedException(in.getSource(), "operation size must be at least 4 but was: " + opSize);
            }
            in.resetDigest(); // size is not part of the checksum!
            if (in.markSupported()) { // if we can we validate the checksum first
                // we are sometimes called when mark is not supported this is the case when
                // we are sending translogs across the network with LZ4 compression enabled - currently there is no way s
                // to prevent this unfortunately.
                in.mark(opSize);

                in.skip(opSize - 4);
                verifyChecksum(in);
                in.reset();
            }
            operation = Translog.Operation.readOperation(in);
            verifyChecksum(in);
        } catch (EOFException e) {
            throw new TruncatedTranslogException(in.getSource(), "reached premature end of file, translog is truncated", e);
        }
        return operation;
    }

    /**
     * Writes all operations in the given iterable to the given output stream including the size of the array
     * use {@link #readOperations(StreamInput, String)} to read it back.
     */
    public static void writeOperations(StreamOutput outStream, List<Operation> toWrite) throws IOException {
        int size = toWrite.size();
        outStream.writeInt(size);
        if (size == 0) {
            return;
        }
        if (outStream.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(outStream);
            for (Operation op : toWrite) {
                writeOperationNoSize(checksumStreamOutput, op);
            }
        } else {
            writeOperationsToStreamLegacyFormat(outStream, toWrite);
        }
    }

    private static void writeOperationsToStreamLegacyFormat(StreamOutput outStream, List<Operation> toWrite) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
        for (Operation op : toWrite) {
            out.reset();
            writeOperationNoSize(checksumStreamOutput, op);
            outStream.writeInt(Math.toIntExact(out.position()));
            out.bytes().writeTo(outStream);
        }
    }

    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Translog.Operation op) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        out.resetDigest();
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    public static void writeOperationWithSize(BytesStreamOutput out, Translog.Operation op) throws IOException {
        final long start = out.position();
        out.skip(Integer.BYTES);
        writeOperationNoSize(new BufferedChecksumStreamOutput(out), op);
        final long end = out.position();
        final int operationSize = (int) (end - Integer.BYTES - start);
        out.seek(start);
        out.writeInt(operationSize);
        out.seek(end);
    }

    /**
     * Gets the minimum generation that could contain any sequence number after the specified sequence number, or the current generation if
     * there is no generation that could any such sequence number.
     *
     * @param seqNo the sequence number
     * @return the minimum generation for the sequence number
     */
    public TranslogGeneration getMinGenerationForSeqNo(final long seqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            return new TranslogGeneration(translogUUID, minGenerationForSeqNo(seqNo, current, readers));
        }
    }

    private static long minGenerationForSeqNo(long seqNo, TranslogWriter writer, List<TranslogReader> readers) {
        long minGen = writer.generation;
        for (final TranslogReader reader : readers) {
            if (seqNo <= reader.getCheckpoint().maxEffectiveSeqNo()) {
                minGen = Math.min(minGen, reader.getGeneration());
            }
        }
        return minGen;
    }

    /**
     * Roll the current translog generation into a new generation if it's not empty. This does not commit the translog.
     *
     * @throws IOException if an I/O exception occurred during any file operations
     */
    public void rollGeneration() throws IOException {
        syncBeforeRollGeneration();
        if (current.totalOperations() == 0 && primaryTermSupplier.getAsLong() == current.getPrimaryTerm()) {
            return;
        }
        try (Releasable ignored = writeLock.acquire()) {
            ensureOpen();
            try {
                final TranslogReader reader = current.closeIntoReader();
                readers.add(reader);
                assert Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME)).generation == current.getGeneration();
                copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                // create a new translog file; this will sync it and update the checkpoint data;
                current = createWriter(current.getGeneration() + 1);
                logger.trace("current translog set to [{}]", current.getGeneration());
            } catch (final Exception e) {
                tragedy.setTragicException(e);
                closeOnTragicEvent(e);
                throw e;
            }
        }
    }

    void syncBeforeRollGeneration() throws IOException {
        // make sure we move most of the data to disk outside of the writeLock
        // in order to reduce the time the lock is held since it's blocking all threads
        sync();
    }

    /**
     * Trims unreferenced translog generations by asking {@link TranslogDeletionPolicy} for the minimum
     * required generation
     */
    public void trimUnreferencedReaders() throws IOException {
        // first check under read lock if any readers can be trimmed
        try (ReleasableLock ignored = readLock.acquire()) {
            if (closed.get()) {
                // we're shutdown potentially on some tragic event, don't delete anything
                return;
            }
            if (getMinReferencedGen() == getMinFileGeneration()) {
                return;
            }
        }

        // move most of the data to disk to reduce the time the write lock is held
        sync();
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get()) {
                // we're shutdown potentially on some tragic event, don't delete anything
                return;
            }
            final long minReferencedGen = getMinReferencedGen();
            for (Iterator<TranslogReader> iterator = readers.iterator(); iterator.hasNext();) {
                TranslogReader reader = iterator.next();
                if (reader.getGeneration() >= minReferencedGen) {
                    break;
                }
                iterator.remove();
                IOUtils.closeWhileHandlingException(reader);
                final Path translogPath = reader.path();
                logger.trace("delete translog file [{}], not referenced and not current anymore", translogPath);
                // The checkpoint is used when opening the translog to know which files should be recovered from.
                // We now update the checkpoint to ignore the file we are going to remove.
                // Note that there is a provision in recoverFromFiles to allow for the case where we synced the checkpoint
                // but crashed before we could delete the file.
                // sync at once to make sure that there's at most one unreferenced generation.
                current.sync();
                deleteReaderFiles(reader);
            }
            assert readers.isEmpty() == false || current.generation == minReferencedGen
                : "all readers were cleaned but the minReferenceGen ["
                    + minReferencedGen
                    + "] is not the current writer's gen ["
                    + current.generation
                    + "]";
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    private long getMinReferencedGen() {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        long minReferencedGen = Math.min(
            deletionPolicy.getMinTranslogGenRequiredByLocks(),
            minGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1, current, readers)
        );
        assert minReferencedGen >= getMinFileGeneration()
            : "deletion policy requires a minReferenceGen of ["
                + minReferencedGen
                + "] but the lowest gen available is ["
                + getMinFileGeneration()
                + "]";
        assert minReferencedGen <= currentFileGeneration()
            : "deletion policy requires a minReferenceGen of ["
                + minReferencedGen
                + "] which is higher than the current generation ["
                + currentFileGeneration()
                + "]";
        return minReferencedGen;
    }

    /**
     * deletes all files associated with a reader. package-private to be able to simulate node failures at this point
     */
    void deleteReaderFiles(TranslogReader reader) {
        IOUtils.deleteFilesIgnoringExceptions(
            reader.path(),
            reader.path().resolveSibling(getCommitCheckpointFileName(reader.getGeneration()))
        );
    }

    void closeFilesIfNoPendingRetentionLocks() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get() && deletionPolicy.pendingTranslogRefCount() == 0) {
                logger.trace("closing files. translog is closed and there are no pending retention locks");
                ArrayList<Closeable> toClose = new ArrayList<>(readers);
                toClose.add(current);
                IOUtils.close(toClose);
            }
        }
    }

    /**
     * References a transaction log generation
     */
    public static final class TranslogGeneration {
        public final String translogUUID;
        public final long translogFileGeneration;

        public TranslogGeneration(String translogUUID, long translogFileGeneration) {
            this.translogUUID = translogUUID;
            this.translogFileGeneration = translogFileGeneration;
        }

    }

    /**
     * Returns the current generation of this translog. This corresponds to the latest uncommitted translog generation
     */
    public TranslogGeneration getGeneration() {
        return new TranslogGeneration(translogUUID, currentFileGeneration());
    }

    long getFirstOperationPosition() { // for testing
        return current.getFirstOperationOffset();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog is already closed", tragedy.get());
        }
    }

    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    /**
     * If this {@code Translog} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Exception getTragicException() {
        return tragedy.get();
    }

    /** Reads and returns the current checkpoint */
    static Checkpoint readCheckpoint(final Path location) throws IOException {
        return Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
    }

    /**
     * Reads the sequence numbers global checkpoint from the translog checkpoint.
     * This ensures that the translogUUID from this translog matches with the provided translogUUID.
     *
     * @param location the location of the translog
     * @return the global checkpoint
     * @throws IOException                if an I/O exception occurred reading the checkpoint
     * @throws TranslogCorruptedException if the translog is corrupted or mismatched with the given uuid
     */
    public static long readGlobalCheckpoint(final Path location, final String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location, expectedTranslogUUID);
        return checkpoint.globalCheckpoint;
    }

    private static Checkpoint readCheckpoint(Path location, String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location);
        // We need to open at least one translog header to validate the translogUUID.
        final Path translogFile = location.resolve(getFilename(checkpoint.generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            TranslogHeader.read(expectedTranslogUUID, translogFile, channel);
        } catch (TranslogCorruptedException ex) {
            throw ex; // just bubble up.
        } catch (Exception ex) {
            throw new TranslogCorruptedException(location.toString(), ex);
        }
        return checkpoint;
    }

    /**
     * Returns the translog uuid used to associate a lucene index with a translog.
     */
    public String getTranslogUUID() {
        return translogUUID;
    }

    /**
     * Returns the max seq_no of translog operations found in this translog. Since this value is calculated based on the current
     * existing readers, this value is not necessary to be the max seq_no of all operations have been stored in this translog.
     */
    public long getMaxSeqNo() {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            final OptionalLong maxSeqNo = Stream.concat(readers.stream(), Stream.of(current))
                .mapToLong(reader -> reader.getCheckpoint().maxSeqNo)
                .max();
            assert maxSeqNo.isPresent() : "must have at least one translog generation";
            return maxSeqNo.getAsLong();
        }
    }

    TranslogWriter getCurrent() {
        return current;
    }

    List<TranslogReader> getReaders() {
        return readers;
    }

    public static String createEmptyTranslog(
        final Path location,
        final long initialGlobalCheckpoint,
        final ShardId shardId,
        final long primaryTerm
    ) throws IOException {
        final ChannelFactory channelFactory = FileChannel::open;
        return createEmptyTranslog(location, initialGlobalCheckpoint, shardId, channelFactory, primaryTerm);
    }

    static String createEmptyTranslog(
        Path location,
        long initialGlobalCheckpoint,
        ShardId shardId,
        ChannelFactory channelFactory,
        long primaryTerm
    ) throws IOException {
        IOUtils.rm(location);
        Files.createDirectories(location);

        final long generation = 1L;
        final long minTranslogGeneration = 1L;
        channelFactory = channelFactory != null ? channelFactory : FileChannel::open;
        final String uuid = Strings.hasLength((String) null) ? null : UUIDs.randomBase64UUID();
        final Path checkpointFile = location.resolve(CHECKPOINT_FILE_NAME);
        final Path translogFile = location.resolve(getFilename(generation));
        final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(0, generation, initialGlobalCheckpoint, minTranslogGeneration);

        // TODO: make this conditional on IndexModule#NODE_STORE_USE_FSYNC
        boolean useFsync = true;
        Checkpoint.write(channelFactory, checkpointFile, checkpoint, useFsync, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        final TranslogWriter writer = TranslogWriter.create(
            shardId,
            uuid,
            generation,
            translogFile,
            channelFactory,
            useFsync,
            EMPTY_TRANSLOG_BUFFER_SIZE,
            minTranslogGeneration,
            initialGlobalCheckpoint,
            () -> {
                throw new UnsupportedOperationException();
            },
            () -> { throw new UnsupportedOperationException(); },
            primaryTerm,
            new TragicExceptionHolder(),
            seqNo -> {
                throw new UnsupportedOperationException();
            },
            BigArrays.NON_RECYCLING_INSTANCE,
            DiskIoBufferPool.INSTANCE,
            (d, s, l) -> {}
        );
        writer.close();
        return uuid;
    }

}
