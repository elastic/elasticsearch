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

package org.elasticsearch.index.translog;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TwoPhaseCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Translog is a per index shard component that records all non-committed index operations in a durable manner.
 * In Elasticsearch there is one Translog instance per {@link org.elasticsearch.index.engine.InternalEngine}. The engine
 * records the current translog generation {@link Translog#getGeneration()} in it's commit metadata using {@link #TRANSLOG_GENERATION_KEY}
 * to reference the generation that contains all operations that have not yet successfully been committed to the engines lucene index.
 * Additionally, since Elasticsearch 2.0 the engine also records a {@link #TRANSLOG_UUID_KEY} with each commit to ensure a strong association
 * between the lucene index an the transaction log file. This UUID is used to prevent accidential recovery from a transaction log that belongs to a
 * different engine.
 * <p>
 * Each Translog has only one translog file open at any time referenced by a translog generation ID. This ID is written to a <tt>translog.ckp</tt> file that is designed
 * to fit in a single disk block such that a write of the file is atomic. The checkpoint file is written on each fsync operation of the translog and records the number of operations
 * written, the current tranlogs file generation and it's fsynced offset in bytes.
 * </p>
 * <p>
 * When a translog is opened the checkpoint is use to retrieve the latest translog file generation and subsequently to open the last written file to recovery operations.
 * The {@link org.elasticsearch.index.translog.Translog.TranslogGeneration} on {@link TranslogConfig#getTranslogGeneration()} given when the translog is opened is compared against
 * the latest generation and all consecutive translog files singe the given generation and the last generation in the checkpoint will be recovered and preserved until the next
 * generation is committed using {@link Translog#commit()}. In the common case the translog file generation in the checkpoint and the generation passed to the translog on creation are
 * the same. The only situation when they can be different is when an actual translog commit fails in between {@link Translog#prepareCommit()} and {@link Translog#commit()}. In such a case
 * the currently being committed translog file will not be deleted since it's commit was not successful. Yet, a new/current translog file is already opened at that point such that there is more than
 * one translog file present. Such an uncommitted translog file always has a <tt>translog-${gen}.ckp</tt> associated with it which is an fsynced copy of the it's last <tt>translog.ckp</tt> such that in
 * disaster recovery last fsynced offsets, number of operation etc. are still preserved.
 * </p>
 */
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable, TwoPhaseCommit {

    /*
     * TODO
     *  - we might need something like a deletion policy to hold on to more than one translog eventually (I think sequence IDs needs this) but we can refactor as we go
     *  - use a simple BufferedOuputStream to write stuff and fold BufferedTranslogWriter into it's super class... the tricky bit is we need to be able to do random access reads even from the buffer
     *  - we need random exception on the FileSystem API tests for all this.
     *  - we need to page align the last write before we sync, we can take advantage of ensureSynced for this since we might have already fsynced far enough
     */
    public static final String TRANSLOG_GENERATION_KEY = "translog_generation";
    public static final String TRANSLOG_UUID_KEY = "translog_uuid";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";
    public static final String CHECKPOINT_SUFFIX = ".ckp";
    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");

    private final List<ImmutableTranslogReader> recoveredTranslogs;
    private volatile ScheduledFuture<?> syncScheduler;
    // this is a concurrent set and is not protected by any of the locks. The main reason
    // is that is being accessed by two separate classes (additions & reading are done by FsTranslog, remove by FsView when closed)
    private final Set<View> outstandingViews = ConcurrentCollections.newConcurrentSet();
    private BigArrays bigArrays;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    private final Path location;
    private TranslogWriter current;
    private volatile ImmutableTranslogReader currentCommittingTranslog;
    private long lastCommittedTranslogFileGeneration = -1; // -1 is safe as it will not cause an translog deletion.
    private final AtomicBoolean closed = new AtomicBoolean();
    private final TranslogConfig config;
    private final String translogUUID;
    private Callback<View> onViewClose = new Callback<View>() {
        @Override
        public void handle(View view) {
            logger.trace("closing view starting at translog [{}]", view.minTranslogGeneration());
            boolean removed = outstandingViews.remove(view);
            assert removed : "View was never set but was supposed to be removed";
        }
    };


    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogConfig} has
     * a non-null {@link org.elasticsearch.index.translog.Translog.TranslogGeneration}. If the generation is null this method
     * us destructive and will delete all files in the translog path given.
     *
     * @see TranslogConfig#getTranslogPath()
     */
    public Translog(TranslogConfig config) throws IOException {
        super(config.getShardId(), config.getIndexSettings());
        this.config = config;
        TranslogGeneration translogGeneration = config.getTranslogGeneration();

        if (translogGeneration == null || translogGeneration.translogUUID == null) { // legacy case
            translogUUID = Strings.randomBase64UUID();
        } else {
            translogUUID = translogGeneration.translogUUID;
        }
        bigArrays = config.getBigArrays();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();
        Files.createDirectories(this.location);
        if (config.getSyncInterval().millis() > 0 && config.getThreadPool() != null) {
            syncScheduler = config.getThreadPool().schedule(config.getSyncInterval(), ThreadPool.Names.SAME, new Sync());
        }

        try {
            if (translogGeneration != null) {
                final Checkpoint checkpoint = Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
                this.recoveredTranslogs = recoverFromFiles(translogGeneration, checkpoint);
                if (recoveredTranslogs.isEmpty()) {
                    throw new IllegalStateException("at least one reader must be recovered");
                }
                current = createWriter(checkpoint.generation + 1);
                this.lastCommittedTranslogFileGeneration = translogGeneration.translogFileGeneration;
            } else {
                this.recoveredTranslogs = Collections.EMPTY_LIST;
                IOUtils.rm(location);
                logger.debug("wipe translog location - creating new translog");
                Files.createDirectories(location);
                final long generation = 1;
                Checkpoint checkpoint = new Checkpoint(0, 0, generation);
                Checkpoint.write(location.resolve(CHECKPOINT_FILE_NAME), checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                current = createWriter(generation);
                this.lastCommittedTranslogFileGeneration = -1; // playing safe

            }
            // now that we know which files are there, create a new current one.
        } catch (Throwable t) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(currentCommittingTranslog, current);
            throw t;
        }
    }

    /** recover all translog files found on disk */
    private final ArrayList<ImmutableTranslogReader> recoverFromFiles(TranslogGeneration translogGeneration, Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<ImmutableTranslogReader> foundTranslogs = new ArrayList<>();
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, TRANSLOG_FILE_SUFFIX); // a temp file to copy checkpoint to - note it must be in on the same FS otherwise atomic move won't work
        boolean tempFileRenamed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);
            final String checkpointTranslogFile = getFilename(checkpoint.generation);
            for (long i = translogGeneration.translogFileGeneration; i < checkpoint.generation; i++) {
                Path committedTranslogFile = location.resolve(getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new IllegalStateException("translog file doesn't exist with generation: " + i + " lastCommitted: " + lastCommittedTranslogFileGeneration + " checkpoint: " + checkpoint.generation + " - translog ids must be consecutive");
                }
                final ImmutableTranslogReader reader = openReader(committedTranslogFile, Checkpoint.read(location.resolve(getCommitCheckpointFileName(i))));
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            foundTranslogs.add(openReader(location.resolve(checkpointTranslogFile), checkpoint));
            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                Checkpoint checkpointFromDisk = Checkpoint.read(commitCheckpoint);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new IllegalStateException("Checkpoint file " + commitCheckpoint.getFileName() + " already exists but has corrupted content expected: " + checkpoint + " but got: " + checkpointFromDisk);
                }
            } else {
                // we first copy this into the temp-file and then fsync it followed by an atomic move into the target file
                // that way if we hit a disk-full here we are still in an consistent state.
                Files.copy(location.resolve(CHECKPOINT_FILE_NAME), tempFile, StandardCopyOption.REPLACE_EXISTING);
                IOUtils.fsync(tempFile, false);
                Files.move(tempFile, commitCheckpoint, StandardCopyOption.ATOMIC_MOVE);
                tempFileRenamed = true;
                // we only fsync the directory the tempFile was already fsynced
                IOUtils.fsync(commitCheckpoint.getParent(), true);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    logger.warn("failed to delete temp file {}", ex, tempFile);
                }
            }
        }
        return foundTranslogs;
    }

    ImmutableTranslogReader openReader(Path path, Checkpoint checkpoint) throws IOException {
        final long generation;
        try {
            generation = parseIdFromFileName(path);
        } catch (IllegalArgumentException ex) {
            throw new TranslogException(shardId, "failed to parse generation from file name matching pattern " + path, ex);
        }
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            final ChannelReference raf = new ChannelReference(path, generation, channel, new OnCloseRunnable());
            ImmutableTranslogReader reader = ImmutableTranslogReader.open(raf, checkpoint, translogUUID);
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
                throw new IllegalStateException("number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " + fileName + "]", e);
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }

    public void updateBuffer(ByteSizeValue bufferSize) {
        config.setBufferSize(bufferSize.bytesAsInt());
        try (ReleasableLock lock = writeLock.acquire()) {
            current.updateBufferSize(config.getBufferSize());
        }
    }

    boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    IOUtils.close(current, currentCommittingTranslog);
                } finally {
                    IOUtils.close(recoveredTranslogs);
                    recoveredTranslogs.clear();
                }
            } finally {
                FutureUtils.cancel(syncScheduler);
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
        try (ReleasableLock lock = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    /**
     * Returns the number of operations in the transaction files that aren't committed to lucene..
     * Note: may return -1 if unknown
     */
    public int totalOperations() {
        int ops = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            ops += current.totalOperations();
            if (currentCommittingTranslog != null) {
                int tops = currentCommittingTranslog.totalOperations();
                assert tops != TranslogReader.UNKNOWN_OP_COUNT;
                assert tops >= 0;
                ops += tops;
            }
        }
        return ops;
    }

    /**
     * Returns the size in bytes of the translog files that aren't committed to lucene.
     */
    public long sizeInBytes() {
        long size = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            size += current.sizeInBytes();
            if (currentCommittingTranslog != null) {
                size += currentCommittingTranslog.sizeInBytes();
            }
        }
        return size;
    }


    TranslogWriter createWriter(long fileGeneration) throws IOException {
        TranslogWriter newFile;
        try {
            newFile = TranslogWriter.create(config.getType(), shardId, translogUUID, fileGeneration, location.resolve(getFilename(fileGeneration)), new OnCloseRunnable(), config.getBufferSize());
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newFile;
    }


    /**
     * Read the Operation object from the given location. This method will try to read the given location from
     * the current or from the currently committing translog file. If the location is in a file that has already
     * been closed or even removed the method will return <code>null</code> instead.
     */
    public Translog.Operation read(Location location) {
        try (ReleasableLock lock = readLock.acquire()) {
            final TranslogReader reader;
            final long currentGeneration = current.getGeneration();
            if (currentGeneration == location.generation) {
                reader = current;
            } else if (currentCommittingTranslog != null && currentCommittingTranslog.getGeneration() == location.generation) {
                reader = currentCommittingTranslog;
            } else if (currentGeneration < location.generation) {
                throw new IllegalStateException("location generation [" + location.generation + "] is greater than the current generation [" + currentGeneration + "]");
            } else {
                return null;
            }
            return reader.read(location);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to read source from translog location " + location, e);
        }
    }

    /**
     * Adds a delete / index operations to the transaction log.
     *
     * @see org.elasticsearch.index.translog.Translog.Operation
     * @see Index
     * @see org.elasticsearch.index.translog.Translog.Delete
     */
    public Location add(Operation operation) throws TranslogException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            final long start = out.position();
            out.skip(RamUsageEstimator.NUM_BYTES_INT);
            writeOperationNoSize(checksumStreamOutput, operation);
            final long end = out.position();
            final int operationSize = (int) (end - RamUsageEstimator.NUM_BYTES_INT - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            final ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock lock = readLock.acquire()) {
                Location location = current.add(bytes);
                if (config.isSyncOnEachOperation()) {
                    current.sync();
                }
                assert current.assertBytesAtLocation(location, bytes);
                return location;
            }
        } catch (Throwable e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            Releasables.close(out.bytes());
        }
    }

    /**
     * Snapshots the current transaction log allowing to safely iterate over the snapshot.
     * Snapshots are fixed in time and will not be updated with future operations.
     */
    public Snapshot newSnapshot() {
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<TranslogReader> toOpen = new ArrayList<>();
            toOpen.addAll(recoveredTranslogs);
            if (currentCommittingTranslog != null) {
                toOpen.add(currentCommittingTranslog);
            }
            toOpen.add(current);
            return createSnapshot(toOpen.toArray(new TranslogReader[toOpen.size()]));
        }
    }

    private static Snapshot createSnapshot(TranslogReader... translogs) {
        Snapshot[] snapshots = new Snapshot[translogs.length];
        boolean success = false;
        try {
            for (int i = 0; i < translogs.length; i++) {
                snapshots[i] = translogs[i].newSnapshot();
            }

            Snapshot snapshot = new MultiSnapshot(snapshots);
            success = true;
            return snapshot;
        } finally {
            if (success == false) {
                Releasables.close(snapshots);
            }
        }
    }

    /**
     * Returns a view into the current translog that is guaranteed to retain all current operations
     * while receiving future ones as well
     */
    public Translog.View newView() {
        // we need to acquire the read lock to make sure no new translog is created
        // and will be missed by the view we're making
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<TranslogReader> translogs = new ArrayList<>();
            try {
                if (currentCommittingTranslog != null) {
                    translogs.add(currentCommittingTranslog.clone());
                }
                translogs.add(current.newReaderFromWriter());
                View view = new View(translogs, onViewClose);
                // this is safe as we know that no new translog is being made at the moment
                // (we hold a read lock) and the view will be notified of any future one
                outstandingViews.add(view);
                translogs.clear();
                return view;
            } finally {
                // close if anything happend and we didn't reach the clear
                IOUtils.closeWhileHandlingException(translogs);
            }
        }
    }

    /**
     * Sync's the translog.
     */
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        }
    }

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
     * Ensures that the given location has be synced / written to the underlying storage.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.generation == current.generation) { // if we have a new one it's already synced
                return current.syncUpTo(location.translogLocation + location.size);
            }
        }
        return false;
    }

    /**
     * return stats
     */
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            return new TranslogStats(totalOperations(), sizeInBytes());
        }
    }

    private boolean isReferencedGeneration(long generation) { // used to make decisions if a file can be deleted
        return generation >= lastCommittedTranslogFileGeneration;
    }

    public TranslogConfig getConfig() {
        return config;
    }


    private final class OnCloseRunnable implements Callback<ChannelReference> {
        @Override
        public void handle(ChannelReference channelReference) {
            try (ReleasableLock lock = writeLock.acquire()) {
                if (isReferencedGeneration(channelReference.getGeneration()) == false) {
                    Path translogPath = channelReference.getPath();
                    assert channelReference.getPath().getParent().equals(location) : "translog files must be in the location folder: " + location + " but was: " + translogPath;
                    // if the given translogPath is not the current we can safely delete the file since all references are released
                    logger.trace("delete translog file - not referenced and not current anymore {}", translogPath);
                    IOUtils.deleteFilesIgnoringExceptions(translogPath);
                    IOUtils.deleteFilesIgnoringExceptions(translogPath.resolveSibling(getCommitCheckpointFileName(channelReference.getGeneration())));

                }
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(location)) {
                    for (Path path : stream) {
                        Matcher matcher = PARSE_STRICT_ID_PATTERN.matcher(path.getFileName().toString());
                        if (matcher.matches()) {
                            long generation = Long.parseLong(matcher.group(1));
                            if (isReferencedGeneration(generation) == false) {
                                logger.trace("delete translog file - not referenced and not current anymore {}", path);
                                IOUtils.deleteFilesIgnoringExceptions(path);
                                IOUtils.deleteFilesIgnoringExceptions(path.resolveSibling(getCommitCheckpointFileName(channelReference.getGeneration())));
                            }
                        }
                    }
                } catch (IOException e) {
                    logger.warn("failed to delete unreferenced translog files", e);
                }
            }
        }
    }

    /**
     * a view into the translog, capturing all translog file at the moment of creation
     * and updated with any future translog.
     */
    public static final class View implements Closeable {
        public static final Translog.View EMPTY_VIEW = new View(Collections.EMPTY_LIST, null);

        boolean closed;
        // last in this list is always FsTranslog.current
        final List<TranslogReader> orderedTranslogs;
        private final Callback<View> onClose;

        View(List<TranslogReader> orderedTranslogs, Callback<View> onClose) {
            // clone so we can safely mutate..
            this.orderedTranslogs = new ArrayList<>(orderedTranslogs);
            this.onClose = onClose;
        }

        /**
         * Called by the parent class when ever the current translog changes
         *
         * @param oldCurrent a new read only reader for the old current (should replace the previous reference)
         * @param newCurrent a reader into the new current.
         */
        synchronized void onNewTranslog(TranslogReader oldCurrent, TranslogReader newCurrent) throws IOException {
            // even though the close method removes this view from outstandingViews, there is no synchronisation in place
            // between that operation and an ongoing addition of a new translog, already having an iterator.
            // As such, this method can be called despite of the fact that we are closed. We need to check and ignore.
            if (closed) {
                // we have to close the new references created for as as we will not hold them
                IOUtils.close(oldCurrent, newCurrent);
                return;
            }
            orderedTranslogs.remove(orderedTranslogs.size() - 1).close();
            orderedTranslogs.add(oldCurrent);
            orderedTranslogs.add(newCurrent);
        }

        /** this smallest translog generation in this view */
        public synchronized long minTranslogGeneration() {
            ensureOpen();
            return orderedTranslogs.get(0).getGeneration();
        }

        /**
         * The total number of operations in the view.
         */
        public synchronized int totalOperations() {
            int ops = 0;
            for (TranslogReader translog : orderedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == TranslogReader.UNKNOWN_OP_COUNT) {
                    return -1;
                }
                assert tops >= 0;
                ops += tops;
            }
            return ops;
        }

        /**
         * Returns the size in bytes of the files behind the view.
         */
        public synchronized long sizeInBytes() {
            long size = 0;
            for (TranslogReader translog : orderedTranslogs) {
                size += translog.sizeInBytes();
            }
            return size;
        }

        /** create a snapshot from this view */
        public synchronized Snapshot snapshot() {
            ensureOpen();
            return createSnapshot(orderedTranslogs.toArray(new TranslogReader[orderedTranslogs.size()]));
        }


        void ensureOpen() {
            if (closed) {
                throw new ElasticsearchException("View is already closed");
            }
        }

        @Override
        public void close() {
            final List<TranslogReader> toClose = new ArrayList<>();
            try {
                synchronized (this) {
                    if (closed == false) {
                        try {
                            if (onClose != null) {
                                onClose.handle(this);
                            }
                        } finally {
                            closed = true;
                            toClose.addAll(orderedTranslogs);
                            orderedTranslogs.clear();
                        }
                    }
                }
            } finally {
                try {
                    // Close out of lock to prevent deadlocks between channel close which checks for
                    // references in InternalChannelReference.closeInternal (waiting on a read lock)
                    // and other FsTranslog#newTranslog calling FsView.onNewTranslog (while having a write lock)
                    IOUtils.close(toClose);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to close view", e);
                }
            }
        }
    }

    class Sync implements Runnable {
        @Override
        public void run() {
            // don't re-schedule  if its closed..., we are done
            if (closed.get()) {
                return;
            }
            final ThreadPool threadPool = config.getThreadPool();
            if (syncNeeded()) {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sync();
                        } catch (Exception e) {
                            logger.warn("failed to sync translog", e);
                        }
                        if (closed.get() == false) {
                            syncScheduler = threadPool.schedule(config.getSyncInterval(), ThreadPool.Names.SAME, Sync.this);
                        }
                    }
                });
            } else {
                syncScheduler = threadPool.schedule(config.getSyncInterval(), ThreadPool.Names.SAME, Sync.this);
            }
        }
    }

    public static class Location implements Accountable, Comparable<Location> {

        public final long generation;
        public final long translogLocation;
        public final int size;

        Location(long generation, long translogLocation, int size) {
            this.generation = generation;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2 * RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
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
    public interface Snapshot extends Releasable {

        /**
         * The total number of operations in the translog.
         */
        int estimatedTotalOperations();

        /**
         * Returns the next operation in the snapshot or <code>null</code> if we reached the end.
         */
        Translog.Operation next() throws IOException;

    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    public interface Operation extends Streamable {
        enum Type {
            @Deprecated
            CREATE((byte) 1),
            INDEX((byte) 2),
            DELETE((byte) 3);

            private final byte id;

            private Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch (id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return INDEX;
                    case 3:
                        return DELETE;
                    default:
                        throw new IllegalArgumentException("No type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();

    }

    public static class Source {
        public final BytesReference source;
        public final String routing;
        public final String parent;
        public final long timestamp;
        public final long ttl;

        public Source(BytesReference source, String routing, String parent, long timestamp, long ttl) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    public static class Index implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;

        public Index() {
        }

        public Index(Engine.Index index) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.parent = index.parent();
            this.version = index.version();
            this.timestamp = index.timestamp();
            this.ttl = index.ttl();
            this.versionType = index.versionType();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public BytesReference source() {
            return this.source;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            try {
                if (version >= 1) {
                    if (in.readBoolean()) {
                        routing = in.readString();
                    }
                }
                if (version >= 2) {
                    if (in.readBoolean()) {
                        parent = in.readString();
                    }
                }
                if (version >= 3) {
                    this.version = in.readLong();
                }
                if (version >= 4) {
                    this.timestamp = in.readLong();
                }
                if (version >= 5) {
                    this.ttl = in.readLong();
                }
                if (version >= 6) {
                    this.versionType = VersionType.fromValue(in.readByte());
                }
            } catch (Exception e) {
                throw new ElasticsearchException("failed to read [" + type + "][" + id + "]", e);
            }

            assert versionType.validateVersionForWrites(version);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
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

            if (version != index.version ||
                    timestamp != index.timestamp ||
                    ttl != index.ttl ||
                    id.equals(index.id) == false ||
                    type.equals(index.type) == false ||
                    versionType != index.versionType ||
                    source.equals(index.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return !(parent != null ? !parent.equals(index.parent) : index.parent != null);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + Long.hashCode(timestamp);
            result = 31 * result + Long.hashCode(ttl);
            return result;
        }

        @Override
        public String toString() {
            return "Index{" +
                    "id='" + id + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Delete implements Operation {
        public static final int SERIALIZATION_FORMAT = 2;

        private Term uid;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Delete() {
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid());
            this.version = delete.version();
            this.versionType = delete.versionType();
        }

        public Delete(Term uid) {
            this.uid = uid;
        }

        public Delete(Term uid, long version, VersionType versionType) {
            this.uid = uid;
            this.version = version;
            this.versionType = versionType;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            uid = new Term(in.readString(), in.readString());
            if (version >= 1) {
                this.version = in.readLong();
            }
            if (version >= 2) {
                this.versionType = VersionType.fromValue(in.readByte());
            }
            assert versionType.validateVersionForWrites(version);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(uid.field());
            out.writeString(uid.text());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
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

            return version == delete.version &&
                    uid.equals(delete.uid) &&
                    versionType == delete.versionType;
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" +
                    "uid=" + uid +
                    '}';
        }
    }


    public enum Durabilty {
        /**
         * Async durability - translogs are synced based on a time interval.
         */
        ASYNC,
        /**
         * Request durability - translogs are synced for each high levle request (bulk, index, delete)
         */
        REQUEST;

    }

    private static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    /**
     * Reads a list of operations written with {@link #writeOperations(StreamOutput, List)}
     */
    public static List<Operation> readOperations(StreamInput input) throws IOException {
        ArrayList<Operation> operations = new ArrayList<>();
        int numOps = input.readInt();
        final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(input);
        for (int i = 0; i < numOps; i++) {
            operations.add(readOperation(checksumStreamInput));
        }
        return operations;
    }

    static Translog.Operation readOperation(BufferedChecksumStreamInput in) throws IOException {
        Translog.Operation operation;
        try {
            final int opSize = in.readInt();
            if (opSize < 4) { // 4byte for the checksum
                throw new AssertionError("operation size must be at least 4 but was: " + opSize);
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
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            operation = newOperationFromType(type);
            operation.readFrom(in);
            verifyChecksum(in);
        } catch (EOFException e) {
            throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
        } catch (AssertionError | Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from stream", e);
        }
        return operation;
    }

    /**
     * Writes all operations in the given iterable to the given output stream including the size of the array
     * use {@link #readOperations(StreamInput)} to read it back.
     */
    public static void writeOperations(StreamOutput outStream, List<Operation> toWrite) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(BigArrays.NON_RECYCLING_INSTANCE);
        try {
            outStream.writeInt(toWrite.size());
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            for (Operation op : toWrite) {
                out.reset();
                final long start = out.position();
                out.skip(RamUsageEstimator.NUM_BYTES_INT);
                writeOperationNoSize(checksumStreamOutput, op);
                long end = out.position();
                int operationSize = (int) (out.position() - RamUsageEstimator.NUM_BYTES_INT - start);
                out.seek(start);
                out.writeInt(operationSize);
                out.seek(end);
                ReleasablePagedBytesReference bytes = out.bytes();
                bytes.writeTo(outStream);
            }
        } finally {
            Releasables.close(out.bytes());
        }

    }

    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Translog.Operation op) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        out.resetDigest();
        out.writeByte(op.opType().id());
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    /**
     * Returns a new empty translog operation for the given {@link Translog.Operation.Type}
     */
    static Translog.Operation newOperationFromType(Translog.Operation.Type type) throws IOException {
        switch (type) {
            case CREATE:
                // the deserialization logic in Index was identical to that of Create when create was deprecated
                return new Index();
            case DELETE:
                return new Translog.Delete();
            case INDEX:
                return new Index();
            default:
                throw new IOException("No type for [" + type + "]");
        }
    }


    @Override
    public void prepareCommit() throws IOException {
        ensureOpen();
        try (ReleasableLock lock = writeLock.acquire()) {
            if (currentCommittingTranslog != null) {
                throw new IllegalStateException("already committing a translog with generation: " + currentCommittingTranslog.getGeneration());
            }
            final TranslogWriter oldCurrent = current;
            oldCurrent.sync();
            currentCommittingTranslog = current.immutableReader();
            Path checkpoint = location.resolve(CHECKPOINT_FILE_NAME);
            assert Checkpoint.read(checkpoint).generation == currentCommittingTranslog.getGeneration();
            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(currentCommittingTranslog.getGeneration()));
            Files.copy(checkpoint, commitCheckpoint);
            IOUtils.fsync(commitCheckpoint, false);
            IOUtils.fsync(commitCheckpoint.getParent(), true);
            // create a new translog file - this will sync it and update the checkpoint data;
            current = createWriter(current.getGeneration() + 1);
            // notify all outstanding views of the new translog (no views are created now as
            // we hold a write lock).
            for (View view : outstandingViews) {
                view.onNewTranslog(currentCommittingTranslog.clone(), current.newReaderFromWriter());
            }
            IOUtils.close(oldCurrent);
            logger.trace("current translog set to [{}]", current.getGeneration());
            assert oldCurrent.syncNeeded() == false : "old translog oldCurrent must not need a sync";

        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this); // tragic event
            throw t;
        }
    }

    @Override
    public void commit() throws IOException {
        ensureOpen();
        ImmutableTranslogReader toClose = null;
        try (ReleasableLock lock = writeLock.acquire()) {
            if (currentCommittingTranslog == null) {
                prepareCommit();
            }
            lastCommittedTranslogFileGeneration = current.getGeneration(); // this is important - otherwise old files will not be cleaned up
            if (recoveredTranslogs.isEmpty() == false) {
                IOUtils.close(recoveredTranslogs);
                recoveredTranslogs.clear();
            }
            toClose = this.currentCommittingTranslog;
            this.currentCommittingTranslog = null;
        } finally {
            IOUtils.close(toClose);
        }
    }

    @Override
    public void rollback() throws IOException {
        ensureOpen();
        close();
    }

    /**
     * References a transaction log generation
     */
    public final static class TranslogGeneration {
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
        try (ReleasableLock lock = writeLock.acquire()) {
            return new TranslogGeneration(translogUUID, currentFileGeneration());
        }
    }

    /**
     * Returns <code>true</code> iff the given generation is the current gbeneration of this translog
     */
    public boolean isCurrent(TranslogGeneration generation) {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (generation != null) {
                if (generation.translogUUID.equals(translogUUID) == false) {
                    throw new IllegalArgumentException("commit belongs to a different translog: " + generation.translogUUID + " vs. " + translogUUID);
                }
                return generation.translogFileGeneration == currentFileGeneration();
            }
        }
        return false;
    }

    long getFirstOperationPosition() { // for testing
        return current.getFirstOperationOffset();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog is already closed");
        }
    }

    /**
     * The number of currently open views
     */
    int getNumOpenViews() {
        return outstandingViews.size();
    }

}
