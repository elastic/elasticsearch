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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene46.Lucene46SegmentInfoFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.CloseableIndexComponent;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.distributor.Distributor;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * A Store provides plain access to files written by an elasticsearch index shard. Each shard
 * has a dedicated store that is uses to access Lucene's Directory which represents the lowest level
 * of file abstraction in Lucene used to read and write Lucene indices.
 * This class also provides access to metadata information like checksums for committed files. A committed
 * file is a file that belongs to a segment written by a Lucene commit. Files that have not been committed
 * ie. created during a merge or a shard refresh / NRT reopen are not considered in the MetadataSnapshot.
 *
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
public class Store extends AbstractIndexShardComponent implements CloseableIndexComponent, Closeable {

    private static final String CODEC = "store";
    private static final int VERSION_STACK_TRACE = 1; // we write the stack trace too since 1.4.0
    private static final int VERSION_START = 0;
    private static final int VERSION = VERSION_STACK_TRACE;
    private static final String CORRUPTED = "corrupted_";

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final CodecService codecService;
    private final DirectoryService directoryService;
    private final StoreDirectory directory;
    private final DistributorDirectory distributorDirectory;

    @Inject
    public Store(ShardId shardId, @IndexSettings Settings indexSettings, CodecService codecService, DirectoryService directoryService, Distributor distributor) throws IOException {
        super(shardId, indexSettings);
        this.codecService = codecService;
        this.directoryService = directoryService;
        this.distributorDirectory = new DistributorDirectory(distributor);
        this.directory = new StoreDirectory(distributorDirectory);
    }


    public Directory directory() {
        ensureOpen();
        return directory;
    }

    /**
     * Returns the last committed segments info for this store
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        return readSegmentsInfo(null, directory());
    }

    /**
     * Returns the segments info for the given commit or for the latest commit if the given commit is <code>null</code>
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    private static SegmentInfos readSegmentsInfo(IndexCommit commit, Directory directory) throws IOException {
        try {
            return commit == null ? Lucene.readSegmentInfos(directory) : Lucene.readSegmentInfos(commit, directory);
        } catch (EOFException eof) {
            // TODO this should be caught by lucene - EOF is almost certainly an index corruption
            throw new CorruptIndexException("Read past EOF while reading segment infos", eof);
        } catch (IOException exception) {
            throw exception; // IOExceptions like too many open files are not necessarily a corruption - just bubble it up
        } catch (Exception ex) {
            throw new CorruptIndexException("Hit unexpected exception while reading segment infos", ex);
        }

    }

    final void ensureOpen() { // for testing
        if (this.refCount.get() <= 0) {
            throw new AlreadyClosedException("Store is already closed");
        }
    }

    /**
     * Returns a new MetadataSnapshot for the latest commit in this store or
     * an empty snapshot if no index exists or can not be opened.
     * @throws CorruptIndexException if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     * unexpected exception when opening the index reading the segments file.
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
     * @throws CorruptIndexException if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     * unexpected exception when opening the index reading the segments file.
     * @throws FileNotFoundException if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException if no index / valid commit-point can be found in this store
     */
    public MetadataSnapshot getMetadata() throws IOException {
        return getMetadata(null);
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     *
     * @throws CorruptIndexException if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     * unexpected exception when opening the index reading the segments file.
     * @throws FileNotFoundException if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException if the commit point can't be found in this store
     */
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        ensureOpen();
        failIfCorrupted();
        try {
            return new MetadataSnapshot(commit, distributorDirectory, logger);
        } catch (CorruptIndexException ex) {
            markStoreCorrupted(ex);
            throw ex;
        }
    }

    /**
     * Deletes the content of a shard store. Be careful calling this!.
     */
    public void deleteContent() throws IOException {
        ensureOpen();
        final String[] files = distributorDirectory.listAll();
        IOException lastException = null;
        for (String file : files) {
            try {
                distributorDirectory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    public StoreStats stats() throws IOException {
        ensureOpen();
        return new StoreStats(Directories.estimateSize(directory), directoryService.throttleTimeInNanos());
    }

    public void renameFile(String from, String to) throws IOException {
        ensureOpen();
        distributorDirectory.renameFile(directoryService, from, to);
    }

    /**
     * Returns <tt>true</tt> by default.
     */
    public boolean suggestUseCompoundFile() {
        return false;
    }

    /**
     * Increments the refCount of this Store instance.  RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     *
     * Note: Close can safely be called multiple times.
     * @see #decRef
     * @see #tryIncRef()
     * @throws AlreadyClosedException iff the reference counter can not be incremented.
     */
    public final void incRef() {
        if (tryIncRef() == false) {
            throw new AlreadyClosedException("Store is already closed can't increment refCount current count [" + refCount.get() + "]");
        }
    }

    /**
     * Tries to increment the refCount of this Store instance. This method will return <tt>true</tt> iff the refCount was
     * incremented successfully otherwise <tt>false</tt>. RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     *
     * Note: Close can safely be called multiple times.
     * @see #decRef()
     * @see #incRef()
     */
    public final boolean tryIncRef() {
        do {
            int i = refCount.get();
            if (i > 0) {
                if (refCount.compareAndSet(i, i + 1)) {
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

    /**
     * Decreases the refCount of this Store instance.If the refCount drops to 0, then this
     * store is closed.
     * @see #incRef
     */
    public final void decRef() {
        int i = refCount.decrementAndGet();
        assert i >= 0;
        if (i == 0) {
            closeInternal();
        }

    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // only do this once!
            decRef();
        }
    }

    private void closeInternal() {
        try {
            directory.innerClose(); // this closes the distributorDirectory as well
        } catch (IOException e) {
            logger.debug("failed to close directory", e);
        }
    }


    /**
     * Reads a MetadataSnapshot from the given index locations or returns an empty snapshot if it can't be read.
     * @throws IOException if the index we try to read is corrupted
     */
    public static MetadataSnapshot readMetadataSnapshot(File[] indexLocations, ESLogger logger) throws IOException {
        final Directory[] dirs = new Directory[indexLocations.length];
        try {
            for (int i=0; i< indexLocations.length; i++) {
                dirs[i] = new SimpleFSDirectory(indexLocations[i]);
            }
            DistributorDirectory dir = new DistributorDirectory(dirs);
            failIfCorrupted(dir, new ShardId("", 1));
            return new MetadataSnapshot(null, dir, logger);
        } catch (IndexNotFoundException ex) {
            // that's fine - happens all the time no need to log
        } catch (FileNotFoundException | NoSuchFileException ex) {
            logger.info("Failed to open / find files while reading metadata snapshot");
        } finally {
            IOUtils.close(dirs);
        }
        return MetadataSnapshot.EMPTY;
    }

    /**
     * The returned IndexOutput might validate the files checksum if the file has been written with a newer lucene version
     * and the metadata holds the necessary information to detect that it was been written by Lucene 4.8 or newer. If it has only
     * a legacy checksum, returned IndexOutput will not verify the checksum.
     *
     * Note: Checksums are calculated nevertheless since lucene does it by default sicne version 4.8.0. This method only adds the
     * verification against the checksum in the given metadata and does not add any significant overhead.
     */
    public IndexOutput createVerifyingOutput(final String filename, final IOContext context, final StoreFileMetaData metadata) throws IOException {
        if (metadata.hasLegacyChecksum() || metadata.checksum() == null) {
            logger.debug("create legacy output for {}", filename);
            return directory().createOutput(filename, context);
        }
        assert metadata.writtenBy() != null;
        assert metadata.writtenBy().onOrAfter(Version.LUCENE_48);
        return new VerifyingIndexOutput(metadata, directory().createOutput(filename, context));
    }

    public static void verify(IndexOutput output) throws IOException {
        if (output instanceof VerifyingIndexOutput) {
            ((VerifyingIndexOutput)output).verify();
        }
    }

    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetaData metadata) throws IOException {
        if (metadata.hasLegacyChecksum() || metadata.checksum() == null) {
            logger.debug("open legacy input for {}", filename);
            return directory().openInput(filename, context);
        }
        assert metadata.writtenBy() != null;
        assert metadata.writtenBy().onOrAfter(Version.LUCENE_48);
        return new VerifyingIndexInput(directory().openInput(filename, context));
    }

    public static void verify(IndexInput input) throws IOException {
        if (input instanceof VerifyingIndexInput) {
            ((VerifyingIndexInput)input).verify();
        }
    }

    public boolean checkIntegrity(StoreFileMetaData md) {
        if (md.writtenBy() != null && md.writtenBy().onOrAfter(Version.LUCENE_48)) {
            try (IndexInput input = directory().openInput(md.name(), IOContext.READONCE)) {
                CodecUtil.checksumEntireFile(input);
            } catch (IOException  e) {
                return false;
            }
        }
        return true;
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

    public void failIfCorrupted() throws IOException {
        ensureOpen();
        failIfCorrupted(directory, shardId);
    }

    private static final void failIfCorrupted(Directory directory, ShardId shardId) throws IOException {
        final String[] files = directory.listAll();
        List<CorruptIndexException> ex = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                try(ChecksumIndexInput input = directory.openChecksumInput(file, IOContext.READONCE)) {
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
                    ex.add(new CorruptIndexException(builder.toString()));
                    CodecUtil.checkFooter(input);
                }
            }
        }
        if (ex.isEmpty() == false) {
            ExceptionsHelper.rethrowAndSuppress(ex);
        }
    }

    /**
     * This exists so {@link org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat} can load its boolean setting; can we find a more straightforward way?
     */
    public class StoreDirectory extends FilterDirectory {

        StoreDirectory(Directory delegateDirectory) throws IOException {
            super(delegateDirectory);
        }

        public ShardId shardId() {
            ensureOpen();
            return Store.this.shardId();
        }

        @Nullable
        public CodecService codecService() {
            ensureOpen();
            return Store.this.codecService;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput in = super.openInput(name, context);
            boolean success = false;
            try {
                // Only for backward comp. since we now use Lucene codec compression
                if (name.endsWith(".fdt") || name.endsWith(".tvf")) {
                    Compressor compressor = CompressorFactory.compressor(in);
                    if (compressor != null) {
                        in = compressor.indexInput(in);
                    }
                }
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(in);
                }
            }
            return in;
        }

        @Override
        public void close() throws IOException {
            assert false : "Nobody should close this directory except of the Store itself";
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
     * Represents a snaphshot of the current directory build from the latest Lucene commit.
     * Only files that are part of the last commit are considered in this datastrucutre.
     * For backwards compatibility the snapshot might include legacy checksums that
     * are derived from a dedicated checksum file written by older elasticsearch version pre 1.3
     *
     * Note: This class will ignore the <tt>segments.gen</tt> file since it's optional and might
     * change concurrently for safety reasons.
     *
     * @see StoreFileMetaData
     */
    public final static class MetadataSnapshot implements Iterable<StoreFileMetaData> {
        private final Map<String, StoreFileMetaData> metadata;

        public static final MetadataSnapshot EMPTY = new MetadataSnapshot();

        public MetadataSnapshot(Map<String, StoreFileMetaData> metadata) {
            this.metadata = metadata;
        }

        MetadataSnapshot() {
            this.metadata = Collections.emptyMap();
        }

        MetadataSnapshot(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
            metadata = buildMetadata(commit, directory, logger);
        }

        ImmutableMap<String, StoreFileMetaData> buildMetadata(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
            ImmutableMap.Builder<String, StoreFileMetaData> builder = ImmutableMap.builder();
            Map<String, String> checksumMap = readLegacyChecksums(directory);
            try {
                final SegmentInfos segmentCommitInfos = Store.readSegmentsInfo(commit, directory);
                Version maxVersion = Version.LUCENE_3_0; // we don't know which version was used to write so we take the max version.
                for (SegmentCommitInfo info : segmentCommitInfos) {
                    final Version version = info.info.getVersion();
                    if (version != null && version.onOrAfter(maxVersion)) {
                        maxVersion = version;
                    }
                    for (String file : info.files()) {
                        String legacyChecksum = checksumMap.get(file);
                        if (version.onOrAfter(Version.LUCENE_4_8) && legacyChecksum == null) {
                            checksumFromLuceneFile(directory, file, builder, logger, version, Lucene46SegmentInfoFormat.SI_EXTENSION.equals(IndexFileNames.getExtension(file)));
                        } else {
                            builder.put(file, new StoreFileMetaData(file, directory.fileLength(file), legacyChecksum, null));
                        }
                    }
                }
                final String segmentsFile = segmentCommitInfos.getSegmentsFileName();
                String legacyChecksum = checksumMap.get(segmentsFile);
                if (maxVersion.onOrAfter(Version.LUCENE_4_8) && legacyChecksum == null) {
                    checksumFromLuceneFile(directory, segmentsFile, builder, logger, maxVersion, true);
                } else {
                    builder.put(segmentsFile, new StoreFileMetaData(segmentsFile, directory.fileLength(segmentsFile), legacyChecksum, null));
                }
            } catch (CorruptIndexException ex) {
                throw ex;
            } catch (Throwable ex) {
                try {
                    // Lucene checks the checksum after it tries to lookup the codec etc.
                    // in that case we might get only IAE or similar exceptions while we are really corrupt...
                    // TODO we should check the checksum in lucene if we hit an exception
                    Lucene.checkSegmentInfoIntegrity(directory);
                } catch (CorruptIndexException cex) {
                  cex.addSuppressed(ex);
                  throw cex;
                } catch (Throwable e) {
                    // ignore...
                }

                throw ex;
            }
            return builder.build();
        }

        static Map<String, String> readLegacyChecksums(Directory directory) throws IOException {
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
                        return indexInput.readStringStringMap();
                    }
                }
                return new HashMap<>();
            }
        }

        private static void checksumFromLuceneFile(Directory directory, String file, ImmutableMap.Builder<String, StoreFileMetaData> builder,  ESLogger logger, Version version, boolean readFileAsHash) throws IOException {
            final String checksum;
            final BytesRef fileHash = new BytesRef();
            try (IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                try {
                    if (in.length() < CodecUtil.footerLength()) {
                        // truncated files trigger IAE if we seek negative... these files are really corrupted though
                        throw new CorruptIndexException("Can't retrieve checksum from file: " + file + " file length must be >= " + CodecUtil.footerLength() + " but was: " + in.length());
                    }
                    if (readFileAsHash) {
                       hashFile(fileHash, in);
                    }
                    checksum = digestToString(CodecUtil.retrieveChecksum(in));

                } catch (Throwable ex) {
                    logger.debug("Can retrieve checksum from file [{}]", ex, file);
                    throw ex;
                }
                builder.put(file, new StoreFileMetaData(file, directory.fileLength(file), checksum, version, fileHash));
            }
        }

        /**
         * Computes a strong hash value for small files. Note that this method should only be used for files < 1MB
         */
        public static void hashFile(BytesRef fileHash, IndexInput in) throws IOException {
            final int len = (int)Math.min(1024 * 1024, in.length()); // for safety we limit this to 1MB
            fileHash.offset = 0;
            fileHash.grow(len);
            fileHash.length = len;
            in.readBytes(fileHash.bytes, 0, len);
        }

        /**
         * Computes a strong hash value for small files. Note that this method should only be used for files < 1MB
         */
        public static void hashFile(BytesRef fileHash, BytesRef source) throws IOException {
            final int len = Math.min(1024 * 1024, source.length); // for safety we limit this to 1MB
            fileHash.offset = 0;
            fileHash.grow(len);
            fileHash.length = len;
            System.arraycopy(source.bytes, source.offset, fileHash.bytes, 0, len);
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

        private static final String DEL_FILE_EXTENSION = "del";  // TODO think about how we can detect if this changes?
        private static final String FIELD_INFOS_FILE_EXTENSION = "fnm";

        /**
         * Returns a diff between the two snapshots that can be used for recovery. The given snapshot is treated as the
         * recovery target and this snapshot as the source. The returned diff will hold a list of files that are:
         *  <ul>
         *      <li>identical: they exist in both snapshots and they can be considered the same ie. they don't need to be recovered</li>
         *      <li>different: they exist in both snapshots but their they are not identical</li>
         *      <li>missing: files that exist in the source but not in the target</li>
         *  </ul>
         * This method groups file into per-segment files and per-commit files. A file is treated as
         * identical if and on if all files in it's group are identical. On a per-segment level files for a segment are treated
         * as identical iff:
         * <ul>
         *     <li>all files in this segment have the same checksum</li>
         *     <li>all files in this segment have the same length</li>
         *     <li>the segments <tt>.si</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>.si</tt> file content as it's hash</li>
         * </ul>
         *
         * The <tt>.si</tt> file contains a lot of diagnostics including a timestamp etc. in the future there might be
         * unique segment identifiers in there hardening this method further.
         *
         * The per-commit files handles very similar. A commit is composed of the <tt>segments_N</tt> files as well as generational files like
         * deletes (<tt>_x_y.del</tt>) or field-info (<tt>_x_y.fnm</tt>) files. On a per-commit level files for a commit are treated
         * as identical iff:
         * <ul>
         *     <li>all files belonging to this commit have the same checksum</li>
         *     <li>all files belonging to this commit have the same length</li>
         *     <li>the segments file <tt>segments_N</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>segments_N</tt> file content as it's hash</li>
         * </ul>
         *
         * NOTE: this diff will not contain the <tt>segments.gen</tt> file. This file is omitted on recovery.
         */
        public RecoveryDiff recoveryDiff(MetadataSnapshot recoveryTargetSnapshot) {
            final ImmutableList.Builder<StoreFileMetaData> identical =  ImmutableList.builder();
            final ImmutableList.Builder<StoreFileMetaData> different =  ImmutableList.builder();
            final ImmutableList.Builder<StoreFileMetaData> missing =  ImmutableList.builder();
            final Map<String, List<StoreFileMetaData>> perSegment = new HashMap<>();
            final List<StoreFileMetaData> perCommitStoreFiles = new ArrayList<>();

            for (StoreFileMetaData meta : this) {
                if (IndexFileNames.SEGMENTS_GEN.equals(meta.name())) {
                    continue; // we don't need that file at all
                }
                final String segmentId = IndexFileNames.parseSegmentName(meta.name());
                final String extension = IndexFileNames.getExtension(meta.name());
                assert FIELD_INFOS_FILE_EXTENSION.equals(extension) == false || IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(meta.name())).isEmpty() : "FieldInfos are generational but updateable DV are not supported in elasticsearch";
                if (IndexFileNames.SEGMENTS.equals(segmentId) || DEL_FILE_EXTENSION.equals(extension)) {
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
            RecoveryDiff recoveryDiff = new RecoveryDiff(identical.build(), different.build(), missing.build());
            assert recoveryDiff.size() == this.metadata.size() - (metadata.containsKey(IndexFileNames.SEGMENTS_GEN) ? 1: 0)
                    : "some files are missing recoveryDiff size: [" + recoveryDiff.size() + "] metadata size: [" + this.metadata.size()  + "] contains  segments.gen: [" + metadata.containsKey(IndexFileNames.SEGMENTS_GEN) + "]"   ;
            return recoveryDiff;
        }

        /**
         * Returns the number of files in this snapshot
         */
        public int size() {
            return metadata.size();
        }
    }

    /**
     * A class representing the diff between a recovery source and recovery target
     * @see MetadataSnapshot#recoveryDiff(org.elasticsearch.index.store.Store.MetadataSnapshot)
     */
    public static final class RecoveryDiff {
        /**
         *  Files that exist in both snapshots and they can be considered the same ie. they don't need to be recovered
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
            synchronized (store.distributorDirectory) {
                Map<String, String> stringStringMap = MetadataSnapshot.readLegacyChecksums(store.distributorDirectory);
                stringStringMap.putAll(legacyChecksums);
                if (!stringStringMap.isEmpty()) {
                    writeChecksums(store.directory, stringStringMap);
                }
            }
        }

        synchronized void writeChecksums(Directory directory, Map<String, String> checksums) throws IOException {
            String checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
            while (directory.fileExists(checksumName)) {
                checksumName = CHECKSUMS_PREFIX + System.currentTimeMillis();
            }
            try (IndexOutput output = directory.createOutput(checksumName, IOContext.DEFAULT)) {
                output.writeInt(0); // version
                output.writeStringStringMap(checksums);
            }
            directory.sync(Collections.singleton(checksumName));
        }

        public void clear() {
            this.legacyChecksums.clear();
        }

        public void remove(String name) {
            legacyChecksums.remove(name);
        }
    }

    private static final String CHECKSUMS_PREFIX = "_checksums-";

    public static final boolean isChecksum(String name) {
        // TODO can we drowp .cks
        return name.startsWith(CHECKSUMS_PREFIX) || name.endsWith(".cks"); // bwcomapt - .cks used to be a previous checksum file
    }

    /**
     * Produces a string representation of the given digest value.
     */
    public static String digestToString(long digest) {
        return Long.toString(digest, Character.MAX_RADIX);
    }


    static class VerifyingIndexOutput extends IndexOutput {

        private final StoreFileMetaData metadata;
        private final IndexOutput output;
        private long writtenBytes;
        private final long checksumPosition;
        private String actualChecksum;

        VerifyingIndexOutput(StoreFileMetaData metadata, IndexOutput actualOutput) {
            this.metadata = metadata;
            this.output = actualOutput;
            checksumPosition = metadata.length() - 8; // the last 8 bytes are the checksum
        }

        @Override
        public void flush() throws IOException {
            output.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

        @Override
        public long getFilePointer() {
            return output.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return output.getChecksum();
        }

        @Override
        public long length() throws IOException {
            return output.length();
        }

        /**
         * Verifies the checksum and compares the written length with the expected file length. This method should bec
         * called after all data has been written to this output.
         */
        public void verify() throws IOException {
            if (metadata.checksum().equals(actualChecksum) && writtenBytes == metadata.length()) {
                return;
            }
            throw new CorruptIndexException("verification failed (hardware problem?) : expected=" + metadata.checksum() +
                    " actual=" + actualChecksum + " writtenLength=" + writtenBytes + " expectedLength=" + metadata.length() +
                    " (resource=" + metadata.toString() + ")");
        }

        @Override
        public void writeByte(byte b) throws IOException {
            if (writtenBytes++ == checksumPosition) {
                readAndCompareChecksum();
            }
            output.writeByte(b);
        }

        private void readAndCompareChecksum() throws IOException {
            actualChecksum = digestToString(getChecksum());
            if (!metadata.checksum().equals(actualChecksum)) {
                throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + metadata.checksum() +
                        " actual=" + actualChecksum +
                        " (resource=" + metadata.toString() + ")");
            }
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            if (writtenBytes + length > checksumPosition && actualChecksum == null) {
                assert writtenBytes <= checksumPosition;
                final int bytesToWrite = (int)(checksumPosition-writtenBytes);
                output.writeBytes(b, offset, bytesToWrite);
                readAndCompareChecksum();
                offset += bytesToWrite;
                length -= bytesToWrite;
                writtenBytes += bytesToWrite;
            }
            output.writeBytes(b, offset, length);
            writtenBytes += length;
        }

    }

    /**
     * Index input that calculates checksum as data is read from the input.
     *
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
                int alreadyVerified = (int)Math.max(0, verifiedPosition - pos);
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

        public void verify() throws CorruptIndexException {
            long storedChecksum = getStoredChecksum();
            if (getChecksum() == storedChecksum) {
                return;
            }
            throw new CorruptIndexException("verification failed : calculated=" + Store.digestToString(getChecksum()) +
                    " stored=" + Store.digestToString(storedChecksum));
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
    public void markStoreCorrupted(CorruptIndexException exception) throws IOException {
        ensureOpen();
        if (!isMarkedCorrupted()) {
            String uuid = CORRUPTED + Strings.randomBase64UUID();
            try(IndexOutput output = this.directory().createOutput(uuid, IOContext.DEFAULT)) {
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
}
