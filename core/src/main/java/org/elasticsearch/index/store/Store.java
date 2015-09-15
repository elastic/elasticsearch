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

package org.elasticsearch.index.store;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * A Store provides plain access to files written by an elasticsearch index shard. Each shard
 * has a dedicated store that is uses to access Lucene's Directory which represents the lowest level
 * of file abstraction in Lucene used to read and write Lucene indices.
 * This class also provides access to metadata information like checksums for committed files. A committed
 * file is a file that belongs to a segment written by a Lucene commit. Files that have not been committed
 * ie. created during a merge or a shard refresh / NRT reopen are not considered in the MetadataSnapshot.
 * <p/>
 * Note: If you use a store it's reference count should be increased before using it by calling #incRef and a
 * corresponding #decRef must be called in a try/finally block to release the store again ie.:
 * <pre>
 *      store.incRef();
 *      try {
 *        // use the store...
 *
 *      } finally {
 *          store.decRef();
 *      }
 * </pre>
 */
public class Store extends AbstractIndexShardComponent implements Closeable, RefCounted {

    private static final String CODEC = "store";
    private static final int VERSION_STACK_TRACE = 1; // we write the stack trace too since 1.4.0
    private static final int VERSION_START = 0;
    private static final int VERSION = VERSION_STACK_TRACE;
    private static final String CORRUPTED = "corrupted_";
    public static final String INDEX_STORE_STATS_REFRESH_INTERVAL = "index.store.stats_refresh_interval";

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final StoreDirectory directory;
    private final ReentrantReadWriteLock metadataLock = new ReentrantReadWriteLock();
    private final ShardLock shardLock;
    private final OnClose onClose;
    private final SingleObjectCache<StoreStats> statsCache;

    private final AbstractRefCounted refCounter = new AbstractRefCounted("store") {
        @Override
        protected void closeInternal() {
            // close us once we are done
            Store.this.closeInternal();
        }
    };

    public Store(ShardId shardId, @IndexSettings Settings indexSettings, DirectoryService directoryService, ShardLock shardLock) throws IOException {
        this(shardId, indexSettings, directoryService, shardLock, OnClose.EMPTY);
    }

    @Inject
    public Store(ShardId shardId, @IndexSettings Settings indexSettings, DirectoryService directoryService, ShardLock shardLock, OnClose onClose) throws IOException {
        super(shardId, indexSettings);
        this.directory = new StoreDirectory(directoryService.newDirectory(), Loggers.getLogger("index.store.deletes", indexSettings, shardId));
        this.shardLock = shardLock;
        this.onClose = onClose;
        final TimeValue refreshInterval = indexSettings.getAsTime(INDEX_STORE_STATS_REFRESH_INTERVAL, TimeValue.timeValueSeconds(10));
        this.statsCache = new StoreStatsCache(refreshInterval, directory, directoryService);
        logger.debug("store stats are refreshed with refresh_interval [{}]", refreshInterval);

        assert onClose != null;
        assert shardLock != null;
        assert shardLock.getShardId().equals(shardId);
    }

    public Directory directory() {
        ensureOpen();
        return directory;
    }

    /**
     * Returns the last committed segments info for this store
     *
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        failIfCorrupted();
        try {
            return readSegmentsInfo(null, directory());
        } catch (CorruptIndexException ex) {
            markStoreCorrupted(ex);
            throw ex;
        }
    }

    /**
     * Returns the segments info for the given commit or for the latest commit if the given commit is <code>null</code>
     *
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    private static SegmentInfos readSegmentsInfo(IndexCommit commit, Directory directory) throws IOException {
        assert commit == null || commit.getDirectory() == directory;
        try {
            return commit == null ? Lucene.readSegmentInfos(directory) : Lucene.readSegmentInfos(commit);
        } catch (EOFException eof) {
            // TODO this should be caught by lucene - EOF is almost certainly an index corruption
            throw new CorruptIndexException("Read past EOF while reading segment infos", "commit(" + commit + ")", eof);
        } catch (IOException exception) {
            throw exception; // IOExceptions like too many open files are not necessarily a corruption - just bubble it up
        } catch (Exception ex) {
            throw new CorruptIndexException("Hit unexpected exception while reading segment infos", "commit(" + commit + ")", ex);
        }

    }

    final void ensureOpen() {
        if (this.refCounter.refCount() <= 0) {
            throw new AlreadyClosedException("store is already closed");
        }
    }

    /**
     * Returns a new MetadataSnapshot for the latest commit in this store or
     * an empty snapshot if no index exists or can not be opened.
     *
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     */
    public MetadataSnapshot getMetadataOrEmpty() throws IOException {
        try {
            return getMetadata(null);
        } catch (IndexNotFoundException ex) {
            // that's fine - happens all the time no need to log
        } catch (FileNotFoundException | NoSuchFileException ex) {
            logger.info("Failed to open / find files while reading metadata snapshot");
        }
        return MetadataSnapshot.EMPTY;
    }

    /**
     * Returns a new MetadataSnapshot for the latest commit in this store.
     *
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if no index / valid commit-point can be found in this store
     */
    public MetadataSnapshot getMetadata() throws IOException {
        return getMetadata(null);
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     *
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if the commit point can't be found in this store
     */
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        ensureOpen();
        failIfCorrupted();
        metadataLock.readLock().lock();
        try {
            return new MetadataSnapshot(commit, directory, logger);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            markStoreCorrupted(ex);
            throw ex;
        } finally {
            metadataLock.readLock().unlock();
        }
    }


    /**
     * Renames all the given files form the key of the map to the
     * value of the map. All successfully renamed files are removed from the map in-place.
     */
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        // this works just like a lucene commit - we rename all temp files and once we successfully
        // renamed all the segments we rename the commit to ensure we don't leave half baked commits behind.
        final Map.Entry<String, String>[] entries = tempFileMap.entrySet().toArray(new Map.Entry[tempFileMap.size()]);
        ArrayUtil.timSort(entries, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                String left = o1.getValue();
                String right = o2.getValue();
                if (left.startsWith(IndexFileNames.SEGMENTS) || right.startsWith(IndexFileNames.SEGMENTS)) {
                    if (left.startsWith(IndexFileNames.SEGMENTS) == false) {
                        return -1;
                    } else if (right.startsWith(IndexFileNames.SEGMENTS) == false) {
                        return 1;
                    }
                }
                return left.compareTo(right);
            }
        });
        metadataLock.writeLock().lock();
        // we make sure that nobody fetches the metadata while we do this rename operation here to ensure we don't
        // get exceptions if files are still open.
        try (Lock writeLock = directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (Map.Entry<String, String> entry : entries) {
                String tempFile = entry.getKey();
                String origFile = entry.getValue();
                // first, go and delete the existing ones
                try {
                    directory.deleteFile(origFile);
                } catch (FileNotFoundException | NoSuchFileException e) {
                } catch (Throwable ex) {
                    logger.debug("failed to delete file [{}]", ex, origFile);
                }
                // now, rename the files... and fail it it won't work
                this.renameFile(tempFile, origFile);
                final String remove = tempFileMap.remove(tempFile);
                assert remove != null;
            }
        } finally {
            metadataLock.writeLock().unlock();
        }

    }

    public StoreStats stats() throws IOException {
        ensureOpen();
        return statsCache.getOrRefresh();
    }

    public void renameFile(String from, String to) throws IOException {
        ensureOpen();
        directory.renameFile(from, to);
    }

    /**
     * Increments the refCount of this Store instance.  RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p/>
     * Note: Close can safely be called multiple times.
     *
     * @throws AlreadyClosedException iff the reference counter can not be incremented.
     * @see #decRef
     * @see #tryIncRef()
     */
    @Override
    public final void incRef() {
        refCounter.incRef();
    }

    /**
     * Tries to increment the refCount of this Store instance. This method will return <tt>true</tt> iff the refCount was
     * incremented successfully otherwise <tt>false</tt>. RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p/>
     * Note: Close can safely be called multiple times.
     *
     * @see #decRef()
     * @see #incRef()
     */
    @Override
    public final boolean tryIncRef() {
        return refCounter.tryIncRef();
    }

    /**
     * Decreases the refCount of this Store instance.If the refCount drops to 0, then this
     * store is closed.
     *
     * @see #incRef
     */
    @Override
    public final void decRef() {
        refCounter.decRef();
    }

    @Override
    public void close() {

        if (isClosed.compareAndSet(false, true)) {
            // only do this once!
            decRef();
            logger.debug("store reference count on close: " + refCounter.refCount());
        }
    }

    private void closeInternal() {
        try {
            try {
                directory.innerClose(); // this closes the distributorDirectory as well
            } finally {
                onClose.handle(shardLock);
            }
        } catch (IOException e) {
            logger.debug("failed to close directory", e);
        } finally {
            IOUtils.closeWhileHandlingException(shardLock);
        }
    }


    /**
     * Reads a MetadataSnapshot from the given index locations or returns an empty snapshot if it can't be read.
     *
     * @throws IOException if the index we try to read is corrupted
     */
    public static MetadataSnapshot readMetadataSnapshot(Path indexLocation, ESLogger logger) throws IOException {
        try (Directory dir = new SimpleFSDirectory(indexLocation)) {
            failIfCorrupted(dir, new ShardId("", 1));
            return new MetadataSnapshot(null, dir, logger);
        } catch (IndexNotFoundException ex) {
            // that's fine - happens all the time no need to log
        } catch (FileNotFoundException | NoSuchFileException ex) {
            logger.info("Failed to open / find files while reading metadata snapshot");
        }
        return MetadataSnapshot.EMPTY;
    }

    /**
     * Returns <code>true</code> iff the given location contains an index an the index
     * can be successfully opened. This includes reading the segment infos and possible
     * corruption markers.
     */
    public static boolean canOpenIndex(ESLogger logger, Path indexLocation) throws IOException {
        try {
            tryOpenIndex(indexLocation);
        } catch (Exception ex) {
            logger.trace("Can't open index for path [{}]", ex, indexLocation);
            return false;
        }
        return true;
    }

    /**
     * Tries to open an index for the given location. This includes reading the
     * segment infos and possible corruption markers. If the index can not
     * be opened, an exception is thrown
     */
    public static void tryOpenIndex(Path indexLocation) throws IOException {
        try (Directory dir = new SimpleFSDirectory(indexLocation)) {
            failIfCorrupted(dir, new ShardId("", 1));
            Lucene.readSegmentInfos(dir);
        }
    }

    /**
     * The returned IndexOutput might validate the files checksum if the file has been written with a newer lucene version
     * and the metadata holds the necessary information to detect that it was been written by Lucene 4.8 or newer. If it has only
     * a legacy checksum, returned IndexOutput will not verify the checksum.
     * <p/>
     * Note: Checksums are calculated nevertheless since lucene does it by default sicne version 4.8.0. This method only adds the
     * verification against the checksum in the given metadata and does not add any significant overhead.
     */
    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetaData metadata, final IOContext context) throws IOException {
        IndexOutput output = directory().createOutput(fileName, context);
        boolean success = false;
        try {
            if (metadata.hasLegacyChecksum()) {
                logger.debug("create legacy adler32 output for {}", fileName);
                output = new LegacyVerification.Adler32VerifyingIndexOutput(output, metadata.checksum(), metadata.length());
            } else if (metadata.checksum() == null) {
                // TODO: when the file is a segments_N, we can still CRC-32 + length for more safety
                // its had that checksum forever.
                logger.debug("create legacy length-only output for {}", fileName);
                output = new LegacyVerification.LengthVerifyingIndexOutput(output, metadata.length());
            } else {
                assert metadata.writtenBy() != null;
                assert metadata.writtenBy().onOrAfter(Version.LUCENE_4_8);
                output = new LuceneVerifyingIndexOutput(metadata, output);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(output);
            }
        }
        return output;
    }

    public static void verify(IndexOutput output) throws IOException {
        if (output instanceof VerifyingIndexOutput) {
            ((VerifyingIndexOutput) output).verify();
        }
    }

    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetaData metadata) throws IOException {
        if (metadata.hasLegacyChecksum() || metadata.checksum() == null) {
            logger.debug("open legacy input for {}", filename);
            return directory().openInput(filename, context);
        }
        assert metadata.writtenBy() != null;
        assert metadata.writtenBy().onOrAfter(Version.LUCENE_4_8_0);
        return new VerifyingIndexInput(directory().openInput(filename, context));
    }

    public static void verify(IndexInput input) throws IOException {
        if (input instanceof VerifyingIndexInput) {
            ((VerifyingIndexInput) input).verify();
        }
    }

    public boolean checkIntegrityNoException(StoreFileMetaData md) {
        return checkIntegrityNoException(md, directory());
    }

    public static boolean checkIntegrityNoException(StoreFileMetaData md, Directory directory) {
        try {
            checkIntegrity(md, directory);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void checkIntegrity(final StoreFileMetaData md, final Directory directory) throws IOException {
        try (IndexInput input = directory.openInput(md.name(), IOContext.READONCE)) {
            if (input.length() != md.length()) { // first check the length no matter how old this file is
                throw new CorruptIndexException("expected length=" + md.length() + " != actual length: " + input.length() + " : file truncated?", input);
            }
            if (md.writtenBy() != null && md.writtenBy().onOrAfter(Version.LUCENE_4_8_0)) {
                // throw exception if the file is corrupt
                String checksum = Store.digestToString(CodecUtil.checksumEntireFile(input));
                // throw exception if metadata is inconsistent
                if (!checksum.equals(md.checksum())) {
                    throw new CorruptIndexException("inconsistent metadata: lucene checksum=" + checksum +
                            ", metadata checksum=" + md.checksum(), input);
                }
            } else if (md.hasLegacyChecksum()) {
                // legacy checksum verification - no footer that we need to omit in the checksum!
                final Checksum checksum = new Adler32();
                final byte[] buffer = new byte[md.length() > 4096 ? 4096 : (int) md.length()];
                final long len = input.length();
                long read = 0;
                while (len > read) {
                    final long bytesLeft = len - read;
                    final int bytesToRead = bytesLeft < buffer.length ? (int) bytesLeft : buffer.length;
                    input.readBytes(buffer, 0, bytesToRead, false);
                    checksum.update(buffer, 0, bytesToRead);
                    read += bytesToRead;
                }
                String adler32 = Store.digestToString(checksum.getValue());
                if (!adler32.equals(md.checksum())) {
                    throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + md.checksum() +
                            " actual=" + adler32, input);
                }
            }
        }
    }

    public boolean isMarkedCorrupted() throws IOException {
        ensureOpen();
        /* marking a store as corrupted is basically adding a _corrupted to all
         * the files. This prevent
         */
        final String[] files = directory().listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes all corruption markers from this store.
     */
    public void removeCorruptionMarker() throws IOException {
        ensureOpen();
        final Directory directory = directory();
        IOException firstException = null;
        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                try {
                    directory.deleteFile(file);
                } catch (IOException ex) {
                    if (firstException == null) {
                        firstException = ex;
                    } else {
                        firstException.addSuppressed(ex);
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public void failIfCorrupted() throws IOException {
        ensureOpen();
        failIfCorrupted(directory, shardId);
    }

    private static final void failIfCorrupted(Directory directory, ShardId shardId) throws IOException {
        final String[] files = directory.listAll();
        List<CorruptIndexException> ex = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                try (ChecksumIndexInput input = directory.openChecksumInput(file, IOContext.READONCE)) {
                    int version = CodecUtil.checkHeader(input, CODEC, VERSION_START, VERSION);
                    String msg = input.readString();
                    StringBuilder builder = new StringBuilder(shardId.toString());
                    builder.append(" Preexisting corrupted index [");
                    builder.append(file).append("] caused by: ");
                    builder.append(msg);
                    if (version == VERSION_STACK_TRACE) {
                        builder.append(System.lineSeparator());
                        builder.append(input.readString());
                    }
                    ex.add(new CorruptIndexException(builder.toString(), "preexisting_corruption"));
                    CodecUtil.checkFooter(input);
                }
            }
        }
        if (ex.isEmpty() == false) {
            ExceptionsHelper.rethrowAndSuppress(ex);
        }
    }

    /**
     * This method deletes every file in this store that is not contained in the given source meta data or is a
     * legacy checksum file. After the delete it pulls the latest metadata snapshot from the store and compares it
     * to the given snapshot. If the snapshots are inconsistent an illegal state exception is thrown
     *
     * @param reason         the reason for this cleanup operation logged for each deleted file
     * @param sourceMetaData the metadata used for cleanup. all files in this metadata should be kept around.
     * @throws IOException           if an IOException occurs
     * @throws IllegalStateException if the latest snapshot in this store differs from the given one after the cleanup.
     */
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetaData) throws IOException {
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            final StoreDirectory dir = directory;
            for (String existingFile : dir.listAll()) {
                if (Store.isAutogenerated(existingFile) || sourceMetaData.contains(existingFile)) {
                    continue; // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete checksum)
                }
                try {
                    dir.deleteFile(reason, existingFile);
                    // FNF should not happen since we hold a write lock?
                } catch (IOException ex) {
                    if (existingFile.startsWith(IndexFileNames.SEGMENTS)
                            || existingFile.equals(IndexFileNames.OLD_SEGMENTS_GEN)) {
                        // TODO do we need to also fail this if we can't delete the pending commit file?
                        // if one of those files can't be deleted we better fail the cleanup otherwise we might leave an old commit point around?
                        throw new IllegalStateException("Can't delete " + existingFile + " - cleanup failed", ex);
                    }
                    logger.debug("failed to delete file [{}]", ex, existingFile);
                    // ignore, we don't really care, will get deleted later on
                }
            }
            final Store.MetadataSnapshot metadataOrEmpty = getMetadata();
            verifyAfterCleanup(sourceMetaData, metadataOrEmpty);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    // pkg private for testing
    final void verifyAfterCleanup(MetadataSnapshot sourceMetaData, MetadataSnapshot targetMetaData) {
        final RecoveryDiff recoveryDiff = targetMetaData.recoveryDiff(sourceMetaData);
        if (recoveryDiff.identical.size() != recoveryDiff.size()) {
            if (recoveryDiff.missing.isEmpty()) {
                for (StoreFileMetaData meta : recoveryDiff.different) {
                    StoreFileMetaData local = targetMetaData.get(meta.name());
                    StoreFileMetaData remote = sourceMetaData.get(meta.name());
                    // if we have different files the they must have no checksums otherwise something went wrong during recovery.
                    // we have that problem when we have an empty index is only a segments_1 file then we can't tell if it's a Lucene 4.8 file
                    // and therefore no checksum. That isn't much of a problem since we simply copy it over anyway but those files come out as
                    // different in the diff. That's why we have to double check here again if the rest of it matches.

                    // all is fine this file is just part of a commit or a segment that is different
                    final boolean same = local.isSame(remote);

                    // this check ensures that the two files are consistent ie. if we don't have checksums only the rest needs to match we are just
                    // verifying that we are consistent on both ends source and target
                    final boolean hashAndLengthEqual = (
                            local.checksum() == null
                                    && remote.checksum() == null
                                    && local.hash().equals(remote.hash())
                                    && local.length() == remote.length());
                    final boolean consistent = hashAndLengthEqual || same;
                    if (consistent == false) {
                        logger.debug("Files are different on the recovery target: {} ", recoveryDiff);
                        throw new IllegalStateException("local version: " + local + " is different from remote version after recovery: " + remote, null);
                    }
                }
            } else {
                logger.debug("Files are missing on the recovery target: {} ", recoveryDiff);
                throw new IllegalStateException("Files are missing on the recovery target: [different="
                        + recoveryDiff.different + ", missing=" + recoveryDiff.missing + ']', null);
            }
        }
    }

    /**
     * Returns the current reference count.
     */
    public int refCount() {
        return refCounter.refCount();
    }

    private static final class StoreDirectory extends FilterDirectory {

        private final ESLogger deletesLogger;

        StoreDirectory(Directory delegateDirectory, ESLogger deletesLogger) throws IOException {
            super(delegateDirectory);
            this.deletesLogger = deletesLogger;
        }


        @Override
        public void close() throws IOException {
            assert false : "Nobody should close this directory except of the Store itself";
        }

        public void deleteFile(String msg, String name) throws IOException {
            deletesLogger.trace("{}: delete file {}", msg, name);
            super.deleteFile(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            deleteFile("StoreDirectory.deleteFile", name);
        }

        private void innerClose() throws IOException {
            super.close();
        }

        @Override
        public String toString() {
            return "store(" + in.toString() + ")";
        }

    }

    /**
     * Represents a snapshot of the current directory build from the latest Lucene commit.
     * Only files that are part of the last commit are considered in this datastrucutre.
     * For backwards compatibility the snapshot might include legacy checksums that
     * are derived from a dedicated checksum file written by older elasticsearch version pre 1.3
     * <p/>
     * Note: This class will ignore the <tt>segments.gen</tt> file since it's optional and might
     * change concurrently for safety reasons.
     *
     * @see StoreFileMetaData
     */
    public final static class MetadataSnapshot implements Iterable<StoreFileMetaData>, Writeable<MetadataSnapshot> {
        private static final ESLogger logger = Loggers.getLogger(MetadataSnapshot.class);
        private static final Version FIRST_LUCENE_CHECKSUM_VERSION = Version.LUCENE_4_8;

        private final ImmutableMap<String, StoreFileMetaData> metadata;

        public static final MetadataSnapshot EMPTY = new MetadataSnapshot();

        private final ImmutableMap<String, String> commitUserData;

        private final long numDocs;

        public MetadataSnapshot(Map<String, StoreFileMetaData> metadata, Map<String, String> commitUserData, long numDocs) {
            ImmutableMap.Builder<String, StoreFileMetaData> metaDataBuilder = ImmutableMap.builder();
            this.metadata = metaDataBuilder.putAll(metadata).build();
            ImmutableMap.Builder<String, String> commitUserDataBuilder = ImmutableMap.builder();
            this.commitUserData = commitUserDataBuilder.putAll(commitUserData).build();
            this.numDocs = numDocs;
        }

        MetadataSnapshot() {
            metadata = ImmutableMap.of();
            commitUserData = ImmutableMap.of();
            numDocs = 0;
        }

        MetadataSnapshot(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
            LoadedMetadata loadedMetadata = loadMetadata(commit, directory, logger);
            metadata = loadedMetadata.fileMetadata;
            commitUserData = loadedMetadata.userData;
            numDocs = loadedMetadata.numDocs;
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        public MetadataSnapshot(StreamInput in) throws IOException {
            final int size = in.readVInt();
            final ImmutableMap.Builder<String, StoreFileMetaData> metadataBuilder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                StoreFileMetaData meta = StoreFileMetaData.readStoreFileMetaData(in);
                metadataBuilder.put(meta.name(), meta);
            }
            final ImmutableMap.Builder<String, String> commitUserDataBuilder = ImmutableMap.builder();
            int num = in.readVInt();
            for (int i = num; i > 0; i--) {
                commitUserDataBuilder.put(in.readString(), in.readString());
            }

            this.commitUserData = commitUserDataBuilder.build();
            this.metadata = metadataBuilder.build();
            this.numDocs = in.readLong();
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        /**
         * Returns the number of documents in this store snapshot
         */
        public long getNumDocs() {
            return numDocs;
        }

        static class LoadedMetadata {
            final ImmutableMap<String, StoreFileMetaData> fileMetadata;
            final ImmutableMap<String, String> userData;
            final long numDocs;

            LoadedMetadata(ImmutableMap<String, StoreFileMetaData> fileMetadata, ImmutableMap<String, String> userData, long numDocs) {
                this.fileMetadata = fileMetadata;
                this.userData = userData;
                this.numDocs = numDocs;
            }
        }

        static LoadedMetadata loadMetadata(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
            long numDocs;
            ImmutableMap.Builder<String, StoreFileMetaData> builder = ImmutableMap.builder();
            Map<String, String> checksumMap = readLegacyChecksums(directory).v1();
            ImmutableMap.Builder<String, String> commitUserDataBuilder = ImmutableMap.builder();
            try {
                final SegmentInfos segmentCommitInfos = Store.readSegmentsInfo(commit, directory);
                numDocs = Lucene.getNumDocs(segmentCommitInfos);
                commitUserDataBuilder.putAll(segmentCommitInfos.getUserData());
                Version maxVersion = Version.LUCENE_4_0; // we don't know which version was used to write so we take the max version.
                for (SegmentCommitInfo info : segmentCommitInfos) {
                    final Version version = info.info.getVersion();
                    if (version == null) {
                        // version is written since 3.1+: we should have already hit IndexFormatTooOld.
                        throw new IllegalArgumentException("expected valid version value: " + info.info.toString());
                    }
                    if (version.onOrAfter(maxVersion)) {
                        maxVersion = version;
                    }
                    for (String file : info.files()) {
                        String legacyChecksum = checksumMap.get(file);
                        if (version.onOrAfter(FIRST_LUCENE_CHECKSUM_VERSION)) {
                            checksumFromLuceneFile(directory, file, builder, logger, version, SEGMENT_INFO_EXTENSION.equals(IndexFileNames.getExtension(file)));
                        } else {
                            builder.put(file, new StoreFileMetaData(file, directory.fileLength(file), legacyChecksum, version));
                        }
                    }
                }
                final String segmentsFile = segmentCommitInfos.getSegmentsFileName();
                String legacyChecksum = checksumMap.get(segmentsFile);
                if (maxVersion.onOrAfter(FIRST_LUCENE_CHECKSUM_VERSION)) {
                    checksumFromLuceneFile(directory, segmentsFile, builder, logger, maxVersion, true);
                } else {
                    final BytesRefBuilder fileHash = new BytesRefBuilder();
                    final long length;
                    try (final IndexInput in = directory.openInput(segmentsFile, IOContext.READONCE)) {
                        length = in.length();
                        hashFile(fileHash, new InputStreamIndexInput(in, length), length);
                    }
                    builder.put(segmentsFile, new StoreFileMetaData(segmentsFile, length, legacyChecksum, maxVersion, fileHash.get()));
                }
            } catch (CorruptIndexException | IndexNotFoundException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                // we either know the index is corrupted or it's just not there
                throw ex;
            } catch (Throwable ex) {
                try {
                    // Lucene checks the checksum after it tries to lookup the codec etc.
                    // in that case we might get only IAE or similar exceptions while we are really corrupt...
                    // TODO we should check the checksum in lucene if we hit an exception
                    logger.warn("failed to build store metadata. checking segment info integrity (with commit [{}])",
                            ex, commit == null ? "no" : "yes");
                    Lucene.checkSegmentInfoIntegrity(directory);
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException cex) {
                    cex.addSuppressed(ex);
                    throw cex;
                } catch (Throwable e) {
                    // ignore...
                }

                throw ex;
            }
            return new LoadedMetadata(builder.build(), commitUserDataBuilder.build(), numDocs);
        }

        /**
         * Reads legacy checksum files found in the directory.
         * <p/>
         * Files are expected to start with _checksums- prefix
         * followed by long file version. Only file with the highest version is read, all other files are ignored.
         *
         * @param directory the directory to read checksums from
         * @return a map of file checksums and the checksum file version
         * @throws IOException
         */
        static Tuple<Map<String, String>, Long> readLegacyChecksums(Directory directory) throws IOException {
            synchronized (directory) {
                long lastFound = -1;
                for (String name : directory.listAll()) {
                    if (!isChecksum(name)) {
                        continue;
                    }
                    long current = Long.parseLong(name.substring(CHECKSUMS_PREFIX.length()));
                    if (current > lastFound) {
                        lastFound = current;
                    }
                }
                if (lastFound > -1) {
                    try (IndexInput indexInput = directory.openInput(CHECKSUMS_PREFIX + lastFound, IOContext.READONCE)) {
                        indexInput.readInt(); // version
                        return new Tuple(indexInput.readStringStringMap(), lastFound);
                    }
                }
                return new Tuple(new HashMap<>(), -1l);
            }
        }

        /**
         * Deletes all checksum files with version lower than newVersion.
         *
         * @param directory  the directory to clean
         * @param newVersion the latest checksum file version
         * @throws IOException
         */
        static void cleanLegacyChecksums(Directory directory, long newVersion) throws IOException {
            synchronized (directory) {
                for (String name : directory.listAll()) {
                    if (isChecksum(name)) {
                        long current = Long.parseLong(name.substring(CHECKSUMS_PREFIX.length()));
                        if (current < newVersion) {
                            try {
                                directory.deleteFile(name);
                            } catch (IOException ex) {
                                logger.debug("can't delete old checksum file [{}]", ex, name);
                            }
                        }
                    }
                }
            }
        }

        private static void checksumFromLuceneFile(Directory directory, String file, ImmutableMap.Builder<String, StoreFileMetaData> builder, ESLogger logger, Version version, boolean readFileAsHash) throws IOException {
            final String checksum;
            final BytesRefBuilder fileHash = new BytesRefBuilder();
            try (final IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                final long length;
                try {
                    length = in.length();
                    if (length < CodecUtil.footerLength()) {
                        // truncated files trigger IAE if we seek negative... these files are really corrupted though
                        throw new CorruptIndexException("Can't retrieve checksum from file: " + file + " file length must be >= " + CodecUtil.footerLength() + " but was: " + in.length(), in);
                    }
                    if (readFileAsHash) {
                        final VerifyingIndexInput verifyingIndexInput = new VerifyingIndexInput(in); // additional safety we checksum the entire file we read the hash for...
                        hashFile(fileHash, new InputStreamIndexInput(verifyingIndexInput, length), length);
                        checksum = digestToString(verifyingIndexInput.verify());
                    } else {
                        checksum = digestToString(CodecUtil.retrieveChecksum(in));
                    }

                } catch (Throwable ex) {
                    logger.debug("Can retrieve checksum from file [{}]", ex, file);
                    throw ex;
                }
                builder.put(file, new StoreFileMetaData(file, length, checksum, version, fileHash.get()));
            }
        }

        /**
         * Computes a strong hash value for small files. Note that this method should only be used for files < 1MB
         */
        public static BytesRef hashFile(Directory directory, String file) throws IOException {
            final BytesRefBuilder fileHash = new BytesRefBuilder();
            try (final IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                hashFile(fileHash, new InputStreamIndexInput(in, in.length()), in.length());
            }
            return fileHash.get();
        }


        /**
         * Computes a strong hash value for small files. Note that this method should only be used for files < 1MB
         */
        public static void hashFile(BytesRefBuilder fileHash, InputStream in, long size) throws IOException {
            final int len = (int) Math.min(1024 * 1024, size); // for safety we limit this to 1MB
            fileHash.grow(len);
            fileHash.setLength(len);
            final int readBytes = Streams.readFully(in, fileHash.bytes(), 0, len);
            assert readBytes == len : Integer.toString(readBytes) + " != " + Integer.toString(len);
            assert fileHash.length() == len : Integer.toString(fileHash.length()) + " != " + Integer.toString(len);
        }

        @Override
        public Iterator<StoreFileMetaData> iterator() {
            return metadata.values().iterator();
        }

        public StoreFileMetaData get(String name) {
            return metadata.get(name);
        }

        public Map<String, StoreFileMetaData> asMap() {
            return metadata;
        }

        private static final String DEL_FILE_EXTENSION = "del"; // legacy delete file
        private static final String LIV_FILE_EXTENSION = "liv"; // lucene 5 delete file
        private static final String FIELD_INFOS_FILE_EXTENSION = "fnm";
        private static final String SEGMENT_INFO_EXTENSION = "si";

        /**
         * Returns a diff between the two snapshots that can be used for recovery. The given snapshot is treated as the
         * recovery target and this snapshot as the source. The returned diff will hold a list of files that are:
         * <ul>
         * <li>identical: they exist in both snapshots and they can be considered the same ie. they don't need to be recovered</li>
         * <li>different: they exist in both snapshots but their they are not identical</li>
         * <li>missing: files that exist in the source but not in the target</li>
         * </ul>
         * This method groups file into per-segment files and per-commit files. A file is treated as
         * identical if and on if all files in it's group are identical. On a per-segment level files for a segment are treated
         * as identical iff:
         * <ul>
         * <li>all files in this segment have the same checksum</li>
         * <li>all files in this segment have the same length</li>
         * <li>the segments <tt>.si</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>.si</tt> file content as it's hash</li>
         * </ul>
         * <p/>
         * The <tt>.si</tt> file contains a lot of diagnostics including a timestamp etc. in the future there might be
         * unique segment identifiers in there hardening this method further.
         * <p/>
         * The per-commit files handles very similar. A commit is composed of the <tt>segments_N</tt> files as well as generational files like
         * deletes (<tt>_x_y.del</tt>) or field-info (<tt>_x_y.fnm</tt>) files. On a per-commit level files for a commit are treated
         * as identical iff:
         * <ul>
         * <li>all files belonging to this commit have the same checksum</li>
         * <li>all files belonging to this commit have the same length</li>
         * <li>the segments file <tt>segments_N</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>segments_N</tt> file content as it's hash</li>
         * </ul>
         * <p/>
         * NOTE: this diff will not contain the <tt>segments.gen</tt> file. This file is omitted on recovery.
         */
        public RecoveryDiff recoveryDiff(MetadataSnapshot recoveryTargetSnapshot) {
            final List<StoreFileMetaData> identical = new ArrayList<>();
            final List<StoreFileMetaData> different = new ArrayList<>();
            final List<StoreFileMetaData> missing = new ArrayList<>();
            final Map<String, List<StoreFileMetaData>> perSegment = new HashMap<>();
            final List<StoreFileMetaData> perCommitStoreFiles = new ArrayList<>();

            for (StoreFileMetaData meta : this) {
                if (IndexFileNames.OLD_SEGMENTS_GEN.equals(meta.name())) { // legacy
                    continue; // we don't need that file at all
                }
                final String segmentId = IndexFileNames.parseSegmentName(meta.name());
                final String extension = IndexFileNames.getExtension(meta.name());
                assert FIELD_INFOS_FILE_EXTENSION.equals(extension) == false || IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(meta.name())).isEmpty() : "FieldInfos are generational but updateable DV are not supported in elasticsearch";
                if (IndexFileNames.SEGMENTS.equals(segmentId) || DEL_FILE_EXTENSION.equals(extension) || LIV_FILE_EXTENSION.equals(extension)) {
                    // only treat del files as per-commit files fnm files are generational but only for upgradable DV
                    perCommitStoreFiles.add(meta);
                } else {
                    List<StoreFileMetaData> perSegStoreFiles = perSegment.get(segmentId);
                    if (perSegStoreFiles == null) {
                        perSegStoreFiles = new ArrayList<>();
                        perSegment.put(segmentId, perSegStoreFiles);
                    }
                    perSegStoreFiles.add(meta);
                }
            }
            final ArrayList<StoreFileMetaData> identicalFiles = new ArrayList<>();
            for (List<StoreFileMetaData> segmentFiles : Iterables.concat(perSegment.values(), Collections.singleton(perCommitStoreFiles))) {
                identicalFiles.clear();
                boolean consistent = true;
                for (StoreFileMetaData meta : segmentFiles) {
                    StoreFileMetaData storeFileMetaData = recoveryTargetSnapshot.get(meta.name());
                    if (storeFileMetaData == null) {
                        consistent = false;
                        missing.add(meta);
                    } else if (storeFileMetaData.isSame(meta) == false) {
                        consistent = false;
                        different.add(meta);
                    } else {
                        identicalFiles.add(meta);
                    }
                }
                if (consistent) {
                    identical.addAll(identicalFiles);
                } else {
                    // make sure all files are added - this can happen if only the deletes are different
                    different.addAll(identicalFiles);
                }
            }
            RecoveryDiff recoveryDiff = new RecoveryDiff(Collections.unmodifiableList(identical), Collections.unmodifiableList(different), Collections.unmodifiableList(missing));
            assert recoveryDiff.size() == this.metadata.size() - (metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) ? 1 : 0)
                    : "some files are missing recoveryDiff size: [" + recoveryDiff.size() + "] metadata size: [" + this.metadata.size() + "] contains  segments.gen: [" + metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) + "]";
            return recoveryDiff;
        }

        /**
         * Returns the number of files in this snapshot
         */
        public int size() {
            return metadata.size();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.metadata.size());
            for (StoreFileMetaData meta : this) {
                meta.writeTo(out);
            }
            out.writeVInt(commitUserData.size());
            for (Map.Entry<String, String> entry : commitUserData.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
            out.writeLong(numDocs);
        }

        public Map<String, String> getCommitUserData() {
            return commitUserData;
        }

        /**
         * Returns true iff this metadata contains the given file.
         */
        public boolean contains(String existingFile) {
            return metadata.containsKey(existingFile);
        }

        /**
         * Returns the segments file that this metadata snapshot represents or null if the snapshot is empty.
         */
        public StoreFileMetaData getSegmentsFile() {
            for (StoreFileMetaData file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    return file;
                }
            }
            assert metadata.isEmpty();
            return null;
        }

        private final int numSegmentFiles() { // only for asserts
            int count = 0;
            for (StoreFileMetaData file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    count++;
                }
            }
            return count;
        }

        /**
         * Returns the sync id of the commit point that this MetadataSnapshot represents.
         *
         * @return sync id if exists, else null
         */
        public String getSyncId() {
            return commitUserData.get(Engine.SYNC_COMMIT_ID);
        }

        @Override
        public MetadataSnapshot readFrom(StreamInput in) throws IOException {
            return new MetadataSnapshot(in);
        }
    }

    /**
     * A class representing the diff between a recovery source and recovery target
     *
     * @see MetadataSnapshot#recoveryDiff(org.elasticsearch.index.store.Store.MetadataSnapshot)
     */
    public static final class RecoveryDiff {
        /**
         * Files that exist in both snapshots and they can be considered the same ie. they don't need to be recovered
         */
        public final List<StoreFileMetaData> identical;
        /**
         * Files that exist in both snapshots but their they are not identical
         */
        public final List<StoreFileMetaData> different;
        /**
         * Files that exist in the source but not in the target
         */
        public final List<StoreFileMetaData> missing;

        RecoveryDiff(List<StoreFileMetaData> identical, List<StoreFileMetaData> different, List<StoreFileMetaData> missing) {
            this.identical = identical;
            this.different = different;
            this.missing = missing;
        }

        /**
         * Returns the sum of the files in this diff.
         */
        public int size() {
            return identical.size() + different.size() + missing.size();
        }

        @Override
        public String toString() {
            return "RecoveryDiff{" +
                    "identical=" + identical +
                    ", different=" + different +
                    ", missing=" + missing +
                    '}';
        }
    }

    public final static class LegacyChecksums {
        private final Map<String, String> legacyChecksums = new HashMap<>();

        public void add(StoreFileMetaData metaData) throws IOException {

            if (metaData.hasLegacyChecksum()) {
                synchronized (this) {
                    // we don't add checksums if they were written by LUCENE_48... now we are using the build in mechanism.
                    legacyChecksums.put(metaData.name(), metaData.checksum());
                }
            }
        }

        public synchronized void write(Store store) throws IOException {
            synchronized (store.directory) {
                Tuple<Map<String, String>, Long> tuple = MetadataSnapshot.readLegacyChecksums(store.directory);
                tuple.v1().putAll(legacyChecksums);
                if (!tuple.v1().isEmpty()) {
                    writeChecksums(store.directory, tuple.v1(), tuple.v2());
                }
            }
        }

        synchronized void writeChecksums(Directory directory, Map<String, String> checksums, long lastVersion) throws IOException {
            // Make sure if clock goes backwards we still move version forwards:
            long nextVersion = Math.max(lastVersion+1, System.currentTimeMillis());
            final String checksumName = CHECKSUMS_PREFIX + nextVersion;
            try (IndexOutput output = directory.createOutput(checksumName, IOContext.DEFAULT)) {
                output.writeInt(0); // version
                output.writeStringStringMap(checksums);
            }
            directory.sync(Collections.singleton(checksumName));
            MetadataSnapshot.cleanLegacyChecksums(directory, nextVersion);
        }

        public void clear() {
            this.legacyChecksums.clear();
        }

        public void remove(String name) {
            legacyChecksums.remove(name);
        }
    }

    public static final String CHECKSUMS_PREFIX = "_checksums-";

    public static boolean isChecksum(String name) {
        // TODO can we drowp .cks
        return name.startsWith(CHECKSUMS_PREFIX) || name.endsWith(".cks"); // bwcomapt - .cks used to be a previous checksum file
    }

    /**
     * Returns true if the file is auto-generated by the store and shouldn't be deleted during cleanup.
     * This includes write lock and checksum files
     */
    public static boolean isAutogenerated(String name) {
        return IndexWriter.WRITE_LOCK_NAME.equals(name) || isChecksum(name);
    }

    /**
     * Produces a string representation of the given digest value.
     */
    public static String digestToString(long digest) {
        return Long.toString(digest, Character.MAX_RADIX);
    }


    static class LuceneVerifyingIndexOutput extends VerifyingIndexOutput {

        private final StoreFileMetaData metadata;
        private long writtenBytes;
        private final long checksumPosition;
        private String actualChecksum;

        LuceneVerifyingIndexOutput(StoreFileMetaData metadata, IndexOutput out) {
            super(out);
            this.metadata = metadata;
            checksumPosition = metadata.length() - 8; // the last 8 bytes are the checksum
        }

        @Override
        public void verify() throws IOException {
            if (metadata.checksum().equals(actualChecksum) && writtenBytes == metadata.length()) {
                return;
            }
            throw new CorruptIndexException("verification failed (hardware problem?) : expected=" + metadata.checksum() +
                    " actual=" + actualChecksum + " writtenLength=" + writtenBytes + " expectedLength=" + metadata.length() +
                    " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
        }

        @Override
        public void writeByte(byte b) throws IOException {
            if (writtenBytes++ == checksumPosition) {
                readAndCompareChecksum();
            }
            out.writeByte(b);
        }

        private void readAndCompareChecksum() throws IOException {
            actualChecksum = digestToString(getChecksum());
            if (!metadata.checksum().equals(actualChecksum)) {
                throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + metadata.checksum() +
                        " actual=" + actualChecksum +
                        " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
            }
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            if (writtenBytes + length > checksumPosition && actualChecksum == null) {
                assert writtenBytes <= checksumPosition;
                final int bytesToWrite = (int) (checksumPosition - writtenBytes);
                out.writeBytes(b, offset, bytesToWrite);
                readAndCompareChecksum();
                offset += bytesToWrite;
                length -= bytesToWrite;
                writtenBytes += bytesToWrite;
            }
            out.writeBytes(b, offset, length);
            writtenBytes += length;
        }

    }

    /**
     * Index input that calculates checksum as data is read from the input.
     * <p/>
     * This class supports random access (it is possible to seek backward and forward) in order to accommodate retry
     * mechanism that is used in some repository plugins (S3 for example). However, the checksum is only calculated on
     * the first read. All consecutive reads of the same data are not used to calculate the checksum.
     */
    static class VerifyingIndexInput extends ChecksumIndexInput {
        private final IndexInput input;
        private final Checksum digest;
        private final long checksumPosition;
        private final byte[] checksum = new byte[8];
        private long verifiedPosition = 0;

        public VerifyingIndexInput(IndexInput input) {
            this(input, new BufferedChecksum(new CRC32()));
        }

        public VerifyingIndexInput(IndexInput input, Checksum digest) {
            super("VerifyingIndexInput(" + input + ")");
            this.input = input;
            this.digest = digest;
            checksumPosition = input.length() - 8;
        }

        @Override
        public byte readByte() throws IOException {
            long pos = input.getFilePointer();
            final byte b = input.readByte();
            pos++;
            if (pos > verifiedPosition) {
                if (pos <= checksumPosition) {
                    digest.update(b);
                } else {
                    checksum[(int) (pos - checksumPosition - 1)] = b;
                }
                verifiedPosition = pos;
            }
            return b;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len)
                throws IOException {
            long pos = input.getFilePointer();
            input.readBytes(b, offset, len);
            if (pos + len > verifiedPosition) {
                // Conversion to int is safe here because (verifiedPosition - pos) can be at most len, which is integer
                int alreadyVerified = (int) Math.max(0, verifiedPosition - pos);
                if (pos < checksumPosition) {
                    if (pos + len < checksumPosition) {
                        digest.update(b, offset + alreadyVerified, len - alreadyVerified);
                    } else {
                        int checksumOffset = (int) (checksumPosition - pos);
                        if (checksumOffset - alreadyVerified > 0) {
                            digest.update(b, offset + alreadyVerified, checksumOffset - alreadyVerified);
                        }
                        System.arraycopy(b, offset + checksumOffset, checksum, 0, len - checksumOffset);
                    }
                } else {
                    // Conversion to int is safe here because checksumPosition is (file length - 8) so
                    // (pos - checksumPosition) cannot be bigger than 8 unless we are reading after the end of file
                    assert pos - checksumPosition < 8;
                    System.arraycopy(b, offset, checksum, (int) (pos - checksumPosition), len);
                }
                verifiedPosition = pos + len;
            }
        }

        @Override
        public long getChecksum() {
            return digest.getValue();
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < verifiedPosition) {
                // going within verified region - just seek there
                input.seek(pos);
            } else {
                if (verifiedPosition > getFilePointer()) {
                    // portion of the skip region is verified and portion is not
                    // skipping the verified portion
                    input.seek(verifiedPosition);
                    // and checking unverified
                    skipBytes(pos - verifiedPosition);
                } else {
                    skipBytes(pos - getFilePointer());
                }
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public long getFilePointer() {
            return input.getFilePointer();
        }

        @Override
        public long length() {
            return input.length();
        }

        @Override
        public IndexInput clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        public long getStoredChecksum() {
            return new ByteArrayDataInput(checksum).readLong();
        }

        public long verify() throws CorruptIndexException {
            long storedChecksum = getStoredChecksum();
            if (getChecksum() == storedChecksum) {
                return storedChecksum;
            }
            throw new CorruptIndexException("verification failed : calculated=" + Store.digestToString(getChecksum()) +
                    " stored=" + Store.digestToString(storedChecksum), this);
        }

    }

    public void deleteQuiet(String... files) {
        for (String file : files) {
            try {
                directory().deleteFile(file);
            } catch (Throwable ex) {
                // ignore
            }
        }
    }

    /**
     * Marks this store as corrupted. This method writes a <tt>corrupted_${uuid}</tt> file containing the given exception
     * message. If a store contains a <tt>corrupted_${uuid}</tt> file {@link #isMarkedCorrupted()} will return <code>true</code>.
     */
    public void markStoreCorrupted(IOException exception) throws IOException {
        ensureOpen();
        if (!isMarkedCorrupted()) {
            String uuid = CORRUPTED + Strings.randomBase64UUID();
            try (IndexOutput output = this.directory().createOutput(uuid, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, CODEC, VERSION);
                output.writeString(ExceptionsHelper.detailedMessage(exception, true, 0)); // handles null exception
                output.writeString(ExceptionsHelper.stackTrace(exception));
                CodecUtil.writeFooter(output);
            } catch (IOException ex) {
                logger.warn("Can't mark store as corrupted", ex);
            }
            directory().sync(Collections.singleton(uuid));
        }
    }

    /**
     * A listener that is executed once the store is closed and all references to it are released
     */
    public static interface OnClose extends Callback<ShardLock> {
        static final OnClose EMPTY = new OnClose() {
            /**
             * This method is called while the provided {@link org.elasticsearch.env.ShardLock} is held.
             * This method is only called once after all resources for a store are released.
             */
            @Override
            public void handle(ShardLock Lock) {
            }
        };
    }

    private static class StoreStatsCache extends SingleObjectCache<StoreStats> {
        private final Directory directory;
        private final DirectoryService directoryService;

        public StoreStatsCache(TimeValue refreshInterval, Directory directory, DirectoryService directoryService) throws IOException {
            super(refreshInterval, new StoreStats(estimateSize(directory), directoryService.throttleTimeInNanos()));
            this.directory = directory;
            this.directoryService = directoryService;
        }

        @Override
        protected StoreStats refresh() {
            try {
                return new StoreStats(estimateSize(directory), directoryService.throttleTimeInNanos());
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to refresh store stats", ex);
            }
        }

        private static long estimateSize(Directory directory) throws IOException {
            long estimatedSize = 0;
            String[] files = directory.listAll();
            for (String file : files) {
                try {
                    estimatedSize += directory.fileLength(file);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // ignore, the file is not there no more
                }
            }
            return estimatedSize;
        }
    }
}
