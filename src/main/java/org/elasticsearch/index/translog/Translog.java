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

import com.google.common.collect.Iterables;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable {

    public static ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb");
    public static final String TRANSLOG_ID_KEY = "translog_id";
    public static final String INDEX_TRANSLOG_DURABILITY = "index.translog.durability";
    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    public static final String INDEX_TRANSLOG_BUFFER_SIZE = "index.translog.fs.buffer_size";
    public static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    static final Pattern PARSE_ID_PATTERN = Pattern.compile(TRANSLOG_FILE_PREFIX + "(\\d+)(\\.recovering)?$");
    private final TimeValue syncInterval;
    private volatile ScheduledFuture<?> syncScheduler;
    private volatile Durabilty durabilty = Durabilty.REQUEST;


    // this is a concurrent set and is not protected by any of the locks. The main reason
    // is that is being accessed by two separate classes (additions & reading are done by FsTranslog, remove by FsView when closed)
    private final Set<FsView> outstandingViews = ConcurrentCollections.newConcurrentSet();


    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TranslogFile.Type type = TranslogFile.Type.fromString(settings.get(INDEX_TRANSLOG_FS_TYPE, Translog.this.type.name()));
            if (type != Translog.this.type) {
                logger.info("updating type from [{}] to [{}]", Translog.this.type, type);
                Translog.this.type = type;
            }

            final Durabilty durabilty = Durabilty.getFromSettings(logger, settings, Translog.this.durabilty);
            if (durabilty != Translog.this.durabilty) {
                logger.info("updating durability from [{}] to [{}]", Translog.this.durabilty, durabilty);
                Translog.this.durabilty = durabilty;
            }
        }
    }

    private final IndexSettingsService indexSettingsService;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;

    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;

    private final Path location;

    // protected by the write lock
    private long idGenerator = 1;
    private TranslogFile current;
    // ordered by age
    private final List<ChannelImmutableReader> uncommittedTranslogs = new ArrayList<>();
    private long lastCommittedTranslogId = -1; // -1 is safe as it will not cause an translog deletion.

    private TranslogFile.Type type;

    private boolean syncOnEachOperation = false;

    private volatile int bufferSize;

    private final ApplySettings applySettings = new ApplySettings();

    private final AtomicBoolean closed = new AtomicBoolean();

    public Translog(ShardId shardId, IndexSettingsService indexSettingsService,
                    BigArrays bigArrays, Path location, ThreadPool threadPool) throws IOException {
        this(shardId, indexSettingsService.getSettings(), indexSettingsService, bigArrays, location, threadPool);
    }

    public Translog(ShardId shardId, @IndexSettings Settings indexSettings,
                    BigArrays bigArrays, Path location) throws IOException {
        this(shardId, indexSettings, null, bigArrays, location, null);
    }

    private Translog(ShardId shardId, @IndexSettings Settings indexSettings, @Nullable IndexSettingsService indexSettingsService,
                     BigArrays bigArrays, Path location, @Nullable ThreadPool threadPool) throws IOException {
        super(shardId, indexSettings);
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.durabilty = Durabilty.getFromSettings(logger, indexSettings, durabilty);
        this.indexSettingsService = indexSettingsService;
        this.bigArrays = bigArrays;
        this.location = location;
        Files.createDirectories(this.location);
        this.threadPool = threadPool;

        this.type = TranslogFile.Type.fromString(indexSettings.get(INDEX_TRANSLOG_FS_TYPE, TranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize(INDEX_TRANSLOG_BUFFER_SIZE, ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...

        syncInterval = indexSettings.getAsTime(INDEX_TRANSLOG_SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0 && threadPool != null) {
            this.syncOnEachOperation = false;
            syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, new Sync());
        } else if (syncInterval.millis() == 0) {
            this.syncOnEachOperation = true;
        }

        if (indexSettingsService != null) {
            indexSettingsService.addListener(applySettings);
        }
        try {
            recoverFromFiles();
            // now that we know which files are there, create a new current one.
            current = createTranslogFile(null);
        } catch (Throwable t) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(uncommittedTranslogs);
            throw t;
        }
    }

    /** recover all translog files found on disk */
    private void recoverFromFiles() throws IOException {
        boolean success = false;
        ArrayList<ChannelImmutableReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock lock = writeLock.acquire()) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(location, TRANSLOG_FILE_PREFIX + "[0-9]*")) {
                for (Path file : stream) {
                    final long id = parseIdFromFileName(file);
                    if (id < 0) {
                        throw new TranslogException(shardId, "failed to parse id from file name matching pattern " + file);
                    }
                    idGenerator = Math.max(idGenerator, id + 1);
                    final ChannelReference raf = new InternalChannelReference(id, location.resolve(getFilename(id)), StandardOpenOption.READ);
                    foundTranslogs.add(new ChannelImmutableReader(id, raf, raf.channel().size(), ChannelReader.UNKNOWN_OP_COUNT));
                    logger.debug("found local translog with id [{}]", id);
                }
            }
            CollectionUtil.timSort(foundTranslogs);
            uncommittedTranslogs.addAll(foundTranslogs);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
        }
    }

    /* extracts the translog id from a file name. returns -1 upon failure */
    public static long parseIdFromFileName(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException("number formatting issue in a file that passed PARSE_ID_PATTERN: " + fileName + "]", e);
            }
        }
        return -1;
    }

    public void updateBuffer(ByteSizeValue bufferSize) {
        this.bufferSize = bufferSize.bytesAsInt();
        try (ReleasableLock lock = writeLock.acquire()) {
            current.updateBufferSize(this.bufferSize);
        }
    }

    boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (indexSettingsService != null) {
                indexSettingsService.removeListener(applySettings);
            }

            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    IOUtils.close(this.current);
                } finally {
                    IOUtils.close(uncommittedTranslogs);
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
     * Returns the id of the current transaction log.
     */
    public long currentId() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.translogId();
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
            for (ChannelReader translog : uncommittedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == ChannelReader.UNKNOWN_OP_COUNT) {
                    return ChannelReader.UNKNOWN_OP_COUNT;
                }
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
            for (ChannelReader translog : uncommittedTranslogs) {
                size += translog.sizeInBytes();
            }
        }
        return size;
    }

    /**
     * notifies the translog that translogId was committed as part of the commit data in lucene, together
     * with all operations from previous translogs. This allows releasing all previous translogs.
     *
     * @throws FileNotFoundException if the given translog id can not be found.
     */
    public void markCommitted(final long translogId) throws FileNotFoundException {
        try (ReleasableLock lock = writeLock.acquire()) {
            logger.trace("updating translogs on commit of [{}]", translogId);
            if (translogId < lastCommittedTranslogId) {
                throw new IllegalArgumentException("committed translog id can only go up (current ["
                        + lastCommittedTranslogId + "], got [" + translogId + "]");
            }
            boolean found = false;
            if (current.translogId() == translogId) {
                found = true;
            } else {
                if (translogId > current.translogId()) {
                    throw new IllegalArgumentException("committed translog id must be lower or equal to current id (current ["
                            + current.translogId() + "], got [" + translogId + "]");
                }
            }
            if (found == false) {
                // try to find it in uncommittedTranslogs
                for (ChannelImmutableReader translog : uncommittedTranslogs) {
                    if (translog.translogId() == translogId) {
                        found = true;
                        break;
                    }
                }
            }
            if (found == false) {
                ArrayList<Long> currentIds = new ArrayList<>();
                for (ChannelReader translog : Iterables.concat(uncommittedTranslogs, Collections.singletonList(current))) {
                    currentIds.add(translog.translogId());
                }
                throw new FileNotFoundException("committed translog id can not be found (current ["
                        + Strings.collectionToCommaDelimitedString(currentIds) + "], got [" + translogId + "]");
            }
            lastCommittedTranslogId = translogId;
            while (uncommittedTranslogs.isEmpty() == false && uncommittedTranslogs.get(0).translogId() < translogId) {
                ChannelReader old = uncommittedTranslogs.remove(0);
                logger.trace("removed [{}] from uncommitted translog list", old.translogId());
                try {
                    old.close();
                } catch (IOException e) {
                    logger.error("failed to closed old translog [{}] (committed id [{}])", e, old, translogId);
                }
            }
        }
    }

    /**
     * Creates a new transaction log file internally. That new file will be visible to all outstanding views.
     * The id of the new translog file is returned.
     */
    public long newTranslog() throws TranslogException, IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            final TranslogFile old = current;
            final TranslogFile newFile = createTranslogFile(old);
            current = newFile;
            ChannelImmutableReader reader = old.immutableReader();
            uncommittedTranslogs.add(reader);
            // notify all outstanding views of the new translog (no views are created now as
            // we hold a write lock).
            for (FsView view : outstandingViews) {
                view.onNewTranslog(old.immutableReader(), current.reader());
            }
            IOUtils.close(old);
            logger.trace("current translog set to [{}]", current.translogId());
            return current.translogId();
        }
    }

    protected TranslogFile createTranslogFile(@Nullable TranslogFile reuse) throws IOException {
        TranslogFile newFile;
        long size = Long.MAX_VALUE;
        try {
            long id = idGenerator++;
            newFile = type.create(shardId, id, new InternalChannelReference(id, location.resolve(getFilename(id)), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW), bufferSize);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        if (reuse != null) {
            newFile.reuse(reuse);
        }
        return newFile;
    }


    /**
     * Read the Operation object from the given location, returns null if the
     * Operation could not be read.
     */
    public Translog.Operation read(Location location) {
        try (ReleasableLock lock = readLock.acquire()) {
            ChannelReader reader = null;
            if (current.translogId() == location.translogId) {
                reader = current;
            } else {
                for (ChannelReader translog : uncommittedTranslogs) {
                    if (translog.translogId() == location.translogId) {
                        reader = translog;
                        break;
                    }
                }
            }
            return reader == null ? null : reader.read(location);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to read source from translog location " + location, e);
        }
    }

    /**
     * Adds a create operation to the transaction log.
     */
    public Location add(Operation operation) throws TranslogException {
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            TranslogStreams.writeTranslogOperation(out, operation);
            ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock lock = readLock.acquire()) {
                Location location = current.add(bytes);
                if (syncOnEachOperation) {
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
            // leave one place for current.
            final ChannelReader[] readers = uncommittedTranslogs.toArray(new ChannelReader[uncommittedTranslogs.size() + 1]);
            readers[readers.length - 1] = current;
            return createdSnapshot(readers);
        }
    }

    private Snapshot createdSnapshot(ChannelReader... translogs) {
        ArrayList<ChannelSnapshot> channelSnapshots = new ArrayList<>();
        boolean success = false;
        try {
            for (ChannelReader translog : translogs) {
                channelSnapshots.add(translog.newSnapshot());
            }
            Snapshot snapshot = new TranslogSnapshot(channelSnapshots, logger);
            success = true;
            return snapshot;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channelSnapshots);
            }
        }
    }

    /**
     * Returns a view into the current translog that is guaranteed to retain all current operations
     * while receiving future ones as well
     */
    public Translog.View newView() {
        // we need to acquire the read lock to make sure new translog is created
        // and will be missed by the view we're making
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<ChannelReader> translogs = new ArrayList<>();
            try {
                for (ChannelImmutableReader translog : uncommittedTranslogs) {
                    translogs.add(translog.clone());
                }
                translogs.add(current.reader());
                FsView view = new FsView(translogs);
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
    String getFilename(long translogId) {
        return TRANSLOG_FILE_PREFIX + translogId;
    }


    /**
     * Ensures that the given location has be synced / written to the underlying storage.
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.translogId == current.id) { // if we have a new one it's already synced
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

    private boolean isReferencedTranslogId(long translogId) {
        return translogId >= lastCommittedTranslogId;
    }

    private final class InternalChannelReference extends ChannelReference {
        final long translogId;

        public InternalChannelReference(long translogId, Path file, OpenOption... openOptions) throws IOException {
            super(file, openOptions);
            this.translogId = translogId;
        }

        @Override
        protected void closeInternal() {
            super.closeInternal();
            try (ReleasableLock lock = writeLock.acquire()) {
                if (isReferencedTranslogId(translogId) == false) {
                    // if the given path is not the current we can safely delete the file since all references are released
                    logger.trace("delete translog file - not referenced and not current anymore {}", file());
                    IOUtils.deleteFilesIgnoringExceptions(file());
                }
            }
        }
    }


    /**
     * a view into the translog, capturing all translog file at the moment of creation
     * and updated with any future translog.
     */
    class FsView implements View {

        boolean closed;
        // last in this list is always FsTranslog.current
        final List<ChannelReader> orderedTranslogs;

        FsView(List<ChannelReader> orderedTranslogs) {
            assert orderedTranslogs.isEmpty() == false;
            // clone so we can safely mutate..
            this.orderedTranslogs = new ArrayList<>(orderedTranslogs);
        }

        /**
         * Called by the parent class when ever the current translog changes
         *
         * @param oldCurrent a new read only reader for the old current (should replace the previous reference)
         * @param newCurrent a reader into the new current.
         */
        synchronized void onNewTranslog(ChannelReader oldCurrent, ChannelReader newCurrent) throws IOException {
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

        @Override
        public synchronized long minTranslogId() {
            ensureOpen();
            return orderedTranslogs.get(0).translogId();
        }

        @Override
        public synchronized int totalOperations() {
            int ops = 0;
            for (ChannelReader translog : orderedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == ChannelReader.UNKNOWN_OP_COUNT) {
                    return -1;
                }
                ops += tops;
            }
            return ops;
        }

        @Override
        public synchronized long sizeInBytes() {
            long size = 0;
            for (ChannelReader translog : orderedTranslogs) {
                size += translog.sizeInBytes();
            }
            return size;
        }

        public synchronized Snapshot snapshot() {
            ensureOpen();
            return createdSnapshot(orderedTranslogs.toArray(new ChannelReader[orderedTranslogs.size()]));
        }


        void ensureOpen() {
            if (closed) {
                throw new ElasticsearchException("View is already closed");
            }
        }

        @Override
        public void close() {
            List<ChannelReader> toClose = new ArrayList<>();
            try {
                synchronized (this) {
                    if (closed == false) {
                        logger.trace("closing view starting at translog [{}]", minTranslogId());
                        closed = true;
                        outstandingViews.remove(this);
                        toClose.addAll(orderedTranslogs);
                        orderedTranslogs.clear();
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
                            syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
                        }
                    }
                });
            } else {
                syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
            }
        }
    }

    public static class Location implements Accountable, Comparable<Location> {

        public final long translogId;
        public final long translogLocation;
        public final int size;

        public Location(long translogId, long translogLocation, int size) {
            this.translogId = translogId;
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
            return "[id: " + translogId + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (translogId == o.translogId) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(translogId, o.translogId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Location location = (Location) o;

            if (translogId != location.translogId) return false;
            if (translogLocation != location.translogLocation) return false;
            return size == location.size;

        }

        @Override
        public int hashCode() {
            int result = (int) (translogId ^ (translogId >>> 32));
            result = 31 * result + (int) (translogLocation ^ (translogLocation >>> 32));
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
        public Translog.Operation next() throws IOException;

    }

    /** a view into the current translog that receives all operations from the moment created */
    public interface View extends Releasable {

        /**
         * The total number of operations in the view.
         */
        int totalOperations();

        /**
         * Returns the size in bytes of the files behind the view.
         */
        long sizeInBytes();

        /** create a snapshot from this view */
        Snapshot snapshot();

        /** this smallest translog id in this view */
        long minTranslogId();

    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    public interface Operation extends Streamable {
        enum Type {
            CREATE((byte) 1),
            SAVE((byte) 2),
            DELETE((byte) 3),
            DELETE_BY_QUERY((byte) 4);

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
                        return SAVE;
                    case 3:
                        return DELETE;
                    case 4:
                        return DELETE_BY_QUERY;
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

    public static class Create implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Create() {
        }

        public Create(Engine.Create create) {
            this.id = create.id();
            this.type = create.type();
            this.source = create.source();
            this.routing = create.routing();
            this.parent = create.parent();
            this.timestamp = create.timestamp();
            this.ttl = create.ttl();
            this.version = create.version();
            this.versionType = create.versionType();
        }

        public Create(String type, String id, byte[] source) {
            this.id = id;
            this.type = type;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.CREATE;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String id() {
            return this.id;
        }

        public BytesReference source() {
            return this.source;
        }

        public String type() {
            return this.type;
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

            Create create = (Create) o;

            if (timestamp != create.timestamp ||
                    ttl != create.ttl ||
                    version != create.version ||
                    id.equals(create.id) == false ||
                    type.equals(create.type) == false ||
                    source.equals(create.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(create.routing) : create.routing != null) {
                return false;
            }
            if (parent != null ? !parent.equals(create.parent) : create.parent != null) {
                return false;
            }
            return versionType == create.versionType;

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Create{" +
                    "id='" + id + '\'' +
                    ", type='" + type + '\'' +
                    '}';
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
            return Type.SAVE;
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
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
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
        public Source getSource(){
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
            result = 31 * result + (int) (version ^ (version >>> 32));
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

    /** @deprecated Delete-by-query is removed in 2.0, but we keep this so translog can replay on upgrade. */
    @Deprecated
    public static class DeleteByQuery implements Operation {

        public static final int SERIALIZATION_FORMAT = 2;
        private BytesReference source;
        @Nullable
        private String[] filteringAliases;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.types());
        }

        public DeleteByQuery(BytesReference source, String[] filteringAliases, String... types) {
            this.source = source;
            this.types = types == null ? Strings.EMPTY_ARRAY : types;
            this.filteringAliases = filteringAliases;
        }

        @Override
        public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override
        public long estimateSize() {
            return source.length() + 8;
        }

        public BytesReference source() {
            return this.source;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public String[] types() {
            return this.types;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete_by_query operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            source = in.readBytesReference();
            if (version < 2) {
                // for query_parser_name, which was removed
                if (in.readBoolean()) {
                    in.readString();
                }
            }
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readString();
                }
            }
            if (version >= 1) {
                int aliasesSize = in.readVInt();
                if (aliasesSize > 0) {
                    filteringAliases = new String[aliasesSize];
                    for (int i = 0; i < aliasesSize; i++) {
                        filteringAliases[i] = in.readString();
                    }
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeBytesReference(source);
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeString(type);
            }
            if (filteringAliases != null) {
                out.writeVInt(filteringAliases.length);
                for (String alias : filteringAliases) {
                    out.writeString(alias);
                }
            } else {
                out.writeVInt(0);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DeleteByQuery that = (DeleteByQuery) o;

            if (!Arrays.equals(filteringAliases, that.filteringAliases)) {
                return false;
            }
            if (!Arrays.equals(types, that.types)) {
                return false;
            }
            return source.equals(that.source);
        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + (filteringAliases != null ? Arrays.hashCode(filteringAliases) : 0);
            result = 31 * result + Arrays.hashCode(types);
            return result;
        }

        @Override
        public String toString() {
            return "DeleteByQuery{" +
                    "types=" + Arrays.toString(types) +
                    '}';
        }
    }

    /**
     * Returns the current durability mode of this translog.
     */
    public Durabilty getDurabilty() {
        return durabilty;
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

        public static Durabilty getFromSettings(ESLogger logger, Settings settings, Durabilty defaultValue) {
            final String value = settings.get(INDEX_TRANSLOG_DURABILITY, defaultValue.name());
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                logger.warn("Can't apply {} illegal value: {} using {} instead, use one of: {}", INDEX_TRANSLOG_DURABILITY, value, defaultValue, Arrays.toString(values()));
                return defaultValue;
            }
        }
    }
}
