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

package org.elasticsearch.index.translog.fs;

import com.google.common.collect.Iterables;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.translog.TranslogStreams;
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
public class FsTranslog extends AbstractIndexShardComponent implements Translog, Closeable {

    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    public static final String INDEX_TRANSLOG_BUFFER_SIZE = "index.translog.fs.buffer_size";
    public static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    private static final Pattern PARSE_ID_PATTERN = Pattern.compile(TRANSLOG_FILE_PREFIX + "(\\d+).*");
    private final TimeValue syncInterval;
    private volatile ScheduledFuture<?> syncScheduler;


    // this is a concurrent set and is not protected by any of the locks. The main reason
    // is that is being accessed by two separate classes (additions & reading are done by FsTranslog, remove by FsView when closed)
    private final Set<FsView> outstandingViews = ConcurrentCollections.newConcurrentSet();


    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            FsTranslogFile.Type type = FsTranslogFile.Type.fromString(settings.get(INDEX_TRANSLOG_FS_TYPE, FsTranslog.this.type.name()));
            if (type != FsTranslog.this.type) {
                logger.info("updating type from [{}] to [{}]", FsTranslog.this.type, type);
                FsTranslog.this.type = type;
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
    private FsTranslogFile current;
    // ordered by age
    private final List<FsChannelImmutableReader> uncommittedTranslogs = new ArrayList<>();
    private long lastCommittedTranslogId = -1; // -1 is safe as it will not cause an translog deletion.

    private FsTranslogFile.Type type;

    private boolean syncOnEachOperation = false;

    private volatile int bufferSize;

    private final ApplySettings applySettings = new ApplySettings();

    private final AtomicBoolean closed = new AtomicBoolean();

    public FsTranslog(ShardId shardId, IndexSettingsService indexSettingsService,
                      BigArrays bigArrays, Path location, ThreadPool threadPool) throws IOException {
        this(shardId, indexSettingsService.getSettings(), indexSettingsService, bigArrays, location, threadPool);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings,
                      BigArrays bigArrays, Path location) throws IOException {
        this(shardId, indexSettings, null, bigArrays, location, null);
    }

    private FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, @Nullable IndexSettingsService indexSettingsService,
                       BigArrays bigArrays, Path location, @Nullable ThreadPool threadPool) throws IOException {
        super(shardId, indexSettings);
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());

        this.indexSettingsService = indexSettingsService;
        this.bigArrays = bigArrays;
        this.location = location;
        Files.createDirectories(this.location);
        this.threadPool = threadPool;

        this.type = FsTranslogFile.Type.fromString(indexSettings.get(INDEX_TRANSLOG_FS_TYPE, FsTranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize(INDEX_TRANSLOG_BUFFER_SIZE, ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...

        syncInterval = indexSettings.getAsTime(INDEX_TRANSLOG_SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0 && threadPool != null) {
            syncOnEachOperation(false);
            syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, new Sync());
        } else if (syncInterval.millis() == 0) {
            syncOnEachOperation(true);
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
        ArrayList<FsChannelImmutableReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock lock = writeLock.acquire()) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(location, TRANSLOG_FILE_PREFIX + "[0-9]*")) {
                for (Path file : stream) {
                    final long id = parseIdFromFileName(file);
                    if (id < 0) {
                        throw new TranslogException(shardId, "failed to parse id from file name matching pattern " + file);
                    }
                    idGenerator = Math.max(idGenerator, id + 1);
                    final ChannelReference raf = new InternalChannelReference(id, location.resolve(getFilename(id)), StandardOpenOption.READ);
                    foundTranslogs.add(new FsChannelImmutableReader(id, raf, raf.channel().size(), FsChannelReader.UNKNOWN_OP_COUNT));
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

    @Override
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

    @Override
    public Path location() {
        return location;
    }

    @Override
    public long currentId() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.translogId();
        }
    }

    @Override
    public int totalOperations() {
        int ops = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            ops += current.totalOperations();
            for (FsChannelReader translog : uncommittedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == FsChannelReader.UNKNOWN_OP_COUNT) {
                    return FsChannelReader.UNKNOWN_OP_COUNT;
                }
                ops += tops;
            }
        }
        return ops;
    }

    @Override
    public long sizeInBytes() {
        long size = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            size += current.sizeInBytes();
            for (FsChannelReader translog : uncommittedTranslogs) {
                size += translog.sizeInBytes();
            }
        }
        return size;
    }

    @Override
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
                for (FsChannelImmutableReader translog : uncommittedTranslogs) {
                    if (translog.translogId() == translogId) {
                        found = true;
                        break;
                    }
                }
            }
            if (found == false) {
                ArrayList<Long> currentIds = new ArrayList<>();
                for (FsChannelReader translog : Iterables.concat(uncommittedTranslogs, Collections.singletonList(current))) {
                    currentIds.add(translog.translogId());
                }
                throw new FileNotFoundException("committed translog id can not be found (current ["
                        + Strings.collectionToCommaDelimitedString(currentIds) + "], got [" + translogId + "]");
            }
            lastCommittedTranslogId = translogId;
            while (uncommittedTranslogs.isEmpty() == false && uncommittedTranslogs.get(0).translogId() < translogId) {
                FsChannelReader old = uncommittedTranslogs.remove(0);
                logger.trace("removed [{}] from uncommitted translog list", old.translogId());
                try {
                    old.close();
                } catch (IOException e) {
                    logger.error("failed to closed old translog [{}] (committed id [{}])", e, old, translogId);
                }
            }
        }
    }

    @Override
    public long newTranslog() throws TranslogException, IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            final FsTranslogFile old = current;
            final FsTranslogFile newFile = createTranslogFile(old);
            current = newFile;
            FsChannelImmutableReader reader = old.immutableReader();
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

    protected FsTranslogFile createTranslogFile(@Nullable FsTranslogFile reuse) throws IOException {
        FsTranslogFile newFile;
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
    @Override
    public Translog.Operation read(Location location) {
        try (ReleasableLock lock = readLock.acquire()) {
            FsChannelReader reader = null;
            if (current.translogId() == location.translogId) {
                reader = current;
            } else {
                for (FsChannelReader translog : uncommittedTranslogs) {
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

    @Override
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

    @Override
    public Snapshot newSnapshot() {
        try (ReleasableLock lock = readLock.acquire()) {
            // leave one place for current.
            final FsChannelReader[] readers = uncommittedTranslogs.toArray(new FsChannelReader[uncommittedTranslogs.size() + 1]);
            readers[readers.length - 1] = current;
            return createdSnapshot(readers);
        }
    }

    private Snapshot createdSnapshot(FsChannelReader... translogs) {
        ArrayList<FsChannelSnapshot> channelSnapshots = new ArrayList<>();
        boolean success = false;
        try {
            for (FsChannelReader translog : translogs) {
                channelSnapshots.add(translog.newSnapshot());
            }
            Snapshot snapshot = new FsTranslogSnapshot(channelSnapshots, logger);
            success = true;
            return snapshot;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channelSnapshots);
            }
        }
    }

    @Override
    public Translog.View newView() {
        // we need to acquire the read lock to make sure new translog is created
        // and will be missed by the view we're making
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<FsChannelReader> translogs = new ArrayList<>();
            try {
                for (FsChannelImmutableReader translog : uncommittedTranslogs) {
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

    @Override
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get()) {
                return;
            }
            current.sync();
        }
    }

    @Override
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    @Override
    public void syncOnEachOperation(boolean syncOnEachOperation) {
        this.syncOnEachOperation = syncOnEachOperation;
        if (syncOnEachOperation) {
            type = FsTranslogFile.Type.SIMPLE;
        } else {
            type = FsTranslogFile.Type.BUFFERED;
        }
    }

    /** package private for testing */
    String getFilename(long translogId) {
        return TRANSLOG_FILE_PREFIX + translogId;
    }

    @Override
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
        final List<FsChannelReader> orderedTranslogs;

        FsView(List<FsChannelReader> orderedTranslogs) {
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
        synchronized void onNewTranslog(FsChannelReader oldCurrent, FsChannelReader newCurrent) throws IOException {
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
            for (FsChannelReader translog : orderedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == FsChannelReader.UNKNOWN_OP_COUNT) {
                    return -1;
                }
                ops += tops;
            }
            return ops;
        }

        @Override
        public synchronized long sizeInBytes() {
            long size = 0;
            for (FsChannelReader translog : orderedTranslogs) {
                size += translog.sizeInBytes();
            }
            return size;
        }

        public synchronized Snapshot snapshot() {
            ensureOpen();
            return createdSnapshot(orderedTranslogs.toArray(new FsChannelReader[orderedTranslogs.size()]));
        }


        void ensureOpen() {
            if (closed) {
                throw new ElasticsearchException("View is already closed");
            }
        }

        @Override
        public void close() {
            List<FsChannelReader> toClose = new ArrayList<>();
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
}
