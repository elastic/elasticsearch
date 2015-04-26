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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.translog.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class FsTranslog extends AbstractIndexShardComponent implements Translog {

    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    private static final String TRANSLOG_FILE_PREFIX = "translog-";
    private static final Pattern PARSE_ID_PATTERN = Pattern.compile(TRANSLOG_FILE_PREFIX + "(\\d+).*");

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

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Path location;

    private volatile FsTranslogFile current;
    private volatile FsTranslogFile trans;

    private FsTranslogFile.Type type;

    private boolean syncOnEachOperation = false;

    private volatile int bufferSize;
    private volatile int transientBufferSize;

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService,
                      BigArrays bigArrays, ShardPath shardPath) throws IOException {
        super(shardId, indexSettings);
        this.indexSettingsService = indexSettingsService;
        this.bigArrays = bigArrays;
        this.location = shardPath.resolveTranslog();
        Files.createDirectories(location);
        this.type = FsTranslogFile.Type.fromString(indexSettings.get("index.translog.fs.type", FsTranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize("index.translog.fs.buffer_size", ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...
        this.transientBufferSize = (int) indexSettings.getAsBytesSize("index.translog.fs.transient_buffer_size", ByteSizeValue.parseBytesSizeValue("8k")).bytes();
        indexSettingsService.addListener(applySettings);
    }

    public FsTranslog(ShardId shardId, @IndexSettings Settings indexSettings, Path location) throws IOException {
        super(shardId, indexSettings);
        this.indexSettingsService = null;
        this.location = location;
        Files.createDirectories(location);
        this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        this.type = FsTranslogFile.Type.fromString(indexSettings.get("index.translog.fs.type", FsTranslogFile.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize("index.translog.fs.buffer_size", ByteSizeValue.parseBytesSizeValue("64k")).bytes();
    }

    @Override
    public void updateBuffer(ByteSizeValue bufferSize) {
        this.bufferSize = bufferSize.bytesAsInt();
        rwl.writeLock().lock();
        try {
            FsTranslogFile current1 = this.current;
            if (current1 != null) {
                current1.updateBufferSize(this.bufferSize);
            }
            current1 = this.trans;
            if (current1 != null) {
                current1.updateBufferSize(this.bufferSize);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (indexSettingsService != null) {
            indexSettingsService.removeListener(applySettings);
        }
        rwl.writeLock().lock();
        try {
            IOUtils.close(this.trans, this.current);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public Path location() {
        return location;
    }

    @Override
    public long currentId() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return -1;
        }
        return current1.id();
    }

    @Override
    public int estimatedNumberOfOperations() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.estimatedNumberOfOperations();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public long translogSizeInBytes() {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return 0;
        }
        return current1.translogSizeInBytes();
    }

    @Override
    public int clearUnreferenced() {
        rwl.writeLock().lock();
        int deleted = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(location, TRANSLOG_FILE_PREFIX + "[0-9]*")) {
            for (Path file : stream) {
                if (isReferencedTranslogFile(file) == false) {
                    try {
                        logger.trace("delete unreferenced translog file: " + file);
                        Files.delete(file);
                        deleted++;
                    } catch (Exception ex) {
                        logger.debug("failed to delete " + file, ex);
                    }
                }
            }
        } catch (IOException ex) {
            logger.debug("failed to clear unreferenced files ", ex);
        } finally {
            rwl.writeLock().unlock();
        }
        return deleted;
    }

    @Override
    public void newTranslog(long id) throws TranslogException, IOException {
        rwl.writeLock().lock();
        try {
            FsTranslogFile newFile;
            try {
                newFile = type.create(shardId, id, new InternalChannelReference(location.resolve(getFilename(id)), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW), bufferSize);
            } catch (IOException e) {
                throw new TranslogException(shardId, "failed to create new translog file", e);
            }
            FsTranslogFile old = current;
            current = newFile;
            IOUtils.close(old);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void newTransientTranslog(long id) throws TranslogException {
        rwl.writeLock().lock();
        try {
            assert this.trans == null;
            this.trans = type.create(shardId, id, new InternalChannelReference(location.resolve(getFilename(id)), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW), transientBufferSize);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public void makeTransientCurrent() throws IOException {
        FsTranslogFile old;
        rwl.writeLock().lock();
        try {
            assert this.trans != null;
            old = current;
            this.current = this.trans;
            this.trans = null;
        } finally {
            rwl.writeLock().unlock();
        }
        old.close();
        current.reuse(old);
    }

    @Override
    public void revertTransient() throws IOException {
        rwl.writeLock().lock();
        try {
            final FsTranslogFile toClose = this.trans;
            this.trans = null;
            IOUtils.close(toClose);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * Returns the translog that should be read for the specified location. If
     * the transient or current translog does not match, returns null
     */
    private FsTranslogFile translogForLocation(Location location) {
        if (trans != null && trans.id() == location.translogId) {
            return this.trans;
        }
        if (current.id() == location.translogId) {
            return this.current;
        }
        return null;
    }

    /**
     * Read the Operation object from the given location, returns null if the
     * Operation could not be read.
     */
    @Override
    public Translog.Operation read(Location location) {
        rwl.readLock().lock();
        try {
            FsTranslogFile translog = translogForLocation(location);
            if (translog != null) {
                byte[] data = translog.read(location);
                try (BytesStreamInput in = new BytesStreamInput(data)) {
                    // Return the Operation using the current version of the
                    // stream based on which translog is being read
                    return translog.getStream().read(in);
                }
            }
            return null;
        } catch (IOException e) {
            throw new ElasticsearchException("failed to read source from translog location " + location, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public Location add(Operation operation) throws TranslogException {
        rwl.readLock().lock();
        boolean released = false;
        ReleasableBytesStreamOutput out = null;
        try {
            out = new ReleasableBytesStreamOutput(bigArrays);
            TranslogStreams.writeTranslogOperation(out, operation);
            ReleasablePagedBytesReference bytes = out.bytes();
            Location location = current.add(bytes);
            if (syncOnEachOperation) {
                current.sync();
            }

            assert new BytesArray(current.read(location)).equals(bytes);

            FsTranslogFile trans = this.trans;
            if (trans != null) {
                try {
                    location = trans.add(bytes);
                } catch (ClosedChannelException e) {
                    // ignore
                }
            }
            Releasables.close(bytes);
            released = true;
            return location;
        } catch (Throwable e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            rwl.readLock().unlock();
            if (!released && out != null) {
                Releasables.close(out.bytes());
            }
        }
    }

    @Override
    public FsChannelSnapshot snapshot() throws TranslogException {
        while (true) {
            FsTranslogFile current = this.current;
            FsChannelSnapshot snapshot = current.snapshot();
            if (snapshot != null) {
                return snapshot;
            }
            if (current.closed() && this.current == current) {
                // check if we are closed and if we are still current - then this translog is closed and we can exit
                throw new TranslogException(shardId, "current translog is already closed");
            }
            Thread.yield();
        }
    }

    @Override
    public Snapshot snapshot(Snapshot snapshot) {
        FsChannelSnapshot snap = snapshot();
        if (snap.translogId() == snapshot.translogId()) {
            snap.seekTo(snapshot.position());
        }
        return snap;
    }

    @Override
    public void sync() throws IOException {
        FsTranslogFile current1 = this.current;
        if (current1 == null) {
            return;
        }
        try {
            current1.sync();
        } catch (IOException e) {
            // if we switches translots (!=), then this failure is not relevant
            // we are working on a new translog
            if (this.current == current1) {
                throw e;
            }
        }
    }

    @Override
    public boolean syncNeeded() {
        FsTranslogFile current1 = this.current;
        return current1 != null && current1.syncNeeded();
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

    @Override
    public String getFilename(long translogId) {
        return TRANSLOG_FILE_PREFIX + translogId;
    }

    @Override
    public TranslogStats stats() {
        FsTranslogFile current = this.current;
        if (current == null) {
            return new TranslogStats(0, 0);
        }

        return new TranslogStats(current.estimatedNumberOfOperations(), current.translogSizeInBytes());
    }

    @Override
    public long findLargestPresentTranslogId() throws IOException {
        rwl.readLock().lock();
        try {
            long maxId = this.currentId();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(location, TRANSLOG_FILE_PREFIX + "[0-9]*")) {
                for (Path translogFile : stream) {
                    try {
                        final String fileName = translogFile.getFileName().toString();
                        final Matcher matcher = PARSE_ID_PATTERN.matcher(fileName);
                        if (matcher.matches()) {
                            maxId = Math.max(maxId, Long.parseLong(matcher.group(1)));
                        }
                    } catch (NumberFormatException ex) {
                        logger.warn("Couldn't parse translog id from file " + translogFile + " skipping");
                    }
                }
            }
            return maxId;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public OperationIterator openIterator(long translogId) throws IOException {
        final String translogName = getFilename(translogId);
        Path recoveringTranslogFile = null;
        logger.trace("try open translog file {} locations {}", translogName, location);
        // we have to support .recovering since it's a leftover from previous version but might still be on the filesystem
        // we used to rename the foo into foo.recovering since foo was reused / overwritten but we fixed that in 2.0
        for (Path recoveryFiles : FileSystemUtils.files(location, translogName + "{.recovering,}")) {
            logger.trace("translog file found in {}", recoveryFiles);
            recoveringTranslogFile = recoveryFiles;
        }
        final boolean translogFileExists = recoveringTranslogFile != null && Files.exists(recoveringTranslogFile);
        if (translogFileExists) {
            if (logger.isTraceEnabled()) {
                logger.trace("opening iterator for translog file: {} length: {}", recoveringTranslogFile, Files.size(recoveringTranslogFile));
            }
            final TranslogStream translogStream = TranslogStreams.translogStreamFor(recoveringTranslogFile);
            return new OperationIteratorImpl(logger, translogStream, translogStream.openInput(recoveringTranslogFile));
        }
        logger.trace("translog file NOT found in {}", location);
        throw new FileNotFoundException("no translog file found for id: " + translogId);
    }

    private boolean isReferencedTranslogFile(Path file) {
        final FsTranslogFile theCurrent = this.current;
        final FsTranslogFile theTrans = this.trans;
        return (theCurrent != null && theCurrent.getPath().equals(file)) ||
                (theTrans != null && theTrans.getPath().equals(file));
    }

    private final class InternalChannelReference extends ChannelReference {

        public InternalChannelReference(Path file, OpenOption... openOptions) throws IOException {
            super(file, openOptions);
        }

        @Override
        protected void closeInternal() {
            super.closeInternal();
            rwl.writeLock().lock();
            try {
                if (isReferencedTranslogFile(file()) == false) {
                    // if the given path is not the current we can safely delete the file since all references are released
                    logger.trace("delete translog file - not referenced and not current anymore {}", file());
                    IOUtils.deleteFilesIgnoringExceptions(file());
                }
            } finally {
                rwl.writeLock().unlock();
            }
        }
    }

    /**
     * Iterator for translog operations.
     */
    private static class OperationIteratorImpl implements org.elasticsearch.index.translog.Translog.OperationIterator {

        private final TranslogStream translogStream;
        private final StreamInput input;
        private final ESLogger logger;

        OperationIteratorImpl(ESLogger logger, TranslogStream translogStream, StreamInput input) {
            this.translogStream = translogStream;
            this.input = input;
            this.logger = logger;
        }

        /**
         * Returns the next operation in the translog or <code>null</code> if we reached the end of the stream.
         */
        public Translog.Operation next() throws IOException {
            try {
                if (translogStream instanceof LegacyTranslogStream) {
                    input.readInt(); // ignored opSize
                }
                return translogStream.read(input);
            } catch (TruncatedTranslogException | EOFException e) {
                // ignore, not properly written the last op
                logger.trace("ignoring translog EOF exception, the last operation was not properly written", e);
                return null;
            } catch (IOException e) {
                // ignore, not properly written last op
                logger.trace("ignoring translog IO exception, the last operation was not properly written", e);
                return null;
            }
        }

        @Override
        public void close() throws ElasticsearchException {
            try {
                input.close();
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to close stream input", ex);
            }
        }
    }
}
