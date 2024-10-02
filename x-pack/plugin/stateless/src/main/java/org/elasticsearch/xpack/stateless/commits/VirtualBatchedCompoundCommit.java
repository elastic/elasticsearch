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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.InternalFile;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.isGenerationalFile;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.CURRENT_VERSION;
import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.common.io.Streams.limitStream;

/**
 * Represents a collection of non-uploaded compound commits, where multiple commits can be added and read,
 * ensuring they will all be uploaded as a single blob with fixed offsets within the final batched compound commit.
 *
 * <p>
 *     This class uses ref-counting to ensure that readers can have access to the underlying Lucene segments once
 *     they've acquired a reference through {@link #incRef()}. The acquired reference remains valid until it is
 *     released using the {@link #decRef()} method.
 * </p>
 *
 * <p>
 *     It is expected that after the batched compound commit is written to the store using the
 *     {@link #getFrozenInputStreamForUpload()} method, the caller should promptly invoke {@link #close()} on the input stream and the VBCC
 *     instance. This action releases the acquired Lucene commit reference and facilitates the proper release of associated resources.
 * </p>
 *
 * <p>
 *     This class facilitates the appending of multiple compound commits via {@link #appendCommit(StatelessCommitRef, boolean)}.
 *     When the caller intends to write these commits to the blob store it should use {@link #getFrozenInputStreamForUpload()}.
 * </p>
 *
 * */
public class VirtualBatchedCompoundCommit extends AbstractRefCounted implements Closeable, AbstractBatchedCompoundCommit {
    private static final Logger logger = LogManager.getLogger(VirtualBatchedCompoundCommit.class);

    private final ShardId shardId;
    private final String nodeEphemeralId;
    private final Function<String, BlobLocation> uploadedBlobLocationsSupplier;
    private final NavigableSet<PendingCompoundCommit> pendingCompoundCommits;
    // TODO: the internal files should be added to the corresponding BlobReferences
    private final Map<String, BlobLocation> internalLocations = new ConcurrentHashMap<>();
    // Maps internal data (pending compound commits' headers, files, padding) to their offset in the virtual batched compound commit
    private final NavigableMap<Long, InternalDataReader> internalDataReadersByOffset = new ConcurrentSkipListMap<>();
    private final AtomicLong currentOffset = new AtomicLong();
    private final AtomicReference<Thread> appendingCommitThread = new AtomicReference<>();
    private final BlobFile blobFile;
    private final PrimaryTermAndGeneration primaryTermAndGeneration;
    private final long creationTimeInMillis;
    // VBCC can no longer be appended to once it is frozen
    private volatile boolean frozen = false;

    public VirtualBatchedCompoundCommit(
        ShardId shardId,
        String nodeEphemeralId,
        long primaryTerm,
        long generation,
        Function<String, BlobLocation> uploadedBlobLocationsSupplier,
        LongSupplier timeInMillisSupplier
    ) {
        this.shardId = shardId;
        this.nodeEphemeralId = nodeEphemeralId;
        this.uploadedBlobLocationsSupplier = uploadedBlobLocationsSupplier;
        this.pendingCompoundCommits = new ConcurrentSkipListSet<>();
        this.primaryTermAndGeneration = new PrimaryTermAndGeneration(primaryTerm, generation);
        this.blobFile = new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(generation), primaryTermAndGeneration);
        this.creationTimeInMillis = timeInMillisSupplier.getAsLong();
    }

    /**
     * Freeze the VBCC so that no more CC can be appended. The VBCC is guaranteed to be frozen afterwards.
     * No synchronization is needed for this method because its sole caller is itself synchronized
     * @return {@code true} if the VBCC is frozen by this thread or
     * {@code false} if it is already frozen or concurrently frozen by other threads.
     */
    public boolean freeze() {
        assert assertCompareAndSetFreezeOrAppendingCommitThread(null, Thread.currentThread());
        try {
            assert pendingCompoundCommits.isEmpty() == false : "Cannot freeze an empty virtual batch compound commit";
            if (isFrozen()) {
                return false;
            }
            frozen = true;
            logger.debug("VBCC is successfully frozen");
            return true;
        } finally {
            assert assertCompareAndSetFreezeOrAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    /**
     * Add the specified {@link StatelessCommitRef} as {@link PendingCompoundCommit}
     * No synchronization is needed for this method because its sole caller is itself synchronized
     * @return {@code true} if the append is successful or {@code false} if the VBCC is frozen and cannot be appended to
     */
    public boolean appendCommit(StatelessCommitRef reference, boolean useInternalFilesReplicatedContent) {
        assert assertCompareAndSetFreezeOrAppendingCommitThread(null, Thread.currentThread());
        try {
            return doAppendCommit(reference, useInternalFilesReplicatedContent);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to append commit [" + reference.getPrimaryTerm() + ", " + reference.getGeneration() + "]",
                e
            );
        } finally {
            assert assertCompareAndSetFreezeOrAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    public boolean isFrozen() {
        return frozen;
    }

    private boolean doAppendCommit(StatelessCommitRef reference, boolean useInternalFilesReplicatedContent) throws IOException {
        assert primaryTermAndGeneration.primaryTerm() == reference.getPrimaryTerm();
        assert (pendingCompoundCommits.isEmpty() && primaryTermAndGeneration.generation() == reference.getGeneration())
            || (pendingCompoundCommits.isEmpty() == false && primaryTermAndGeneration.generation() < reference.getGeneration());
        assert pendingCompoundCommits.isEmpty() || pendingCompoundCommits.last().getGeneration() < reference.getGeneration();

        // bail early if VBCC is already frozen to avoid doing any work
        if (isFrozen()) {
            return false;
        }

        final var ccTermAndGen = new PrimaryTermAndGeneration(reference.getPrimaryTerm(), reference.getGeneration());
        final boolean isFirstCommit = ccTermAndGen.equals(primaryTermAndGeneration);

        // Ordered set of compound commit (CC) internal files
        var internalFiles = new TreeSet<InternalFile>();

        // Map of compound commit (CC) referenced files
        var referencedFiles = new HashMap<String, BlobLocation>();

        var internalFilesSize = 0L;
        for (String commitFile : reference.getCommitFiles()) {
            boolean isAdditionalFile = reference.getAdditionalFiles().contains(commitFile);
            if (isAdditionalFile || (isFirstCommit && isGenerationalFile(commitFile))) {
                assert internalLocations.containsKey(commitFile) == false : commitFile;
                var fileLength = reference.getDirectory().fileLength(commitFile);
                internalFiles.add(new InternalFile(commitFile, fileLength));
                internalFilesSize += fileLength;
            } else {
                var blobLocation = internalLocations.get(commitFile);
                assert blobLocation != null || isGenerationalFile(commitFile) == false : commitFile;
                if (blobLocation == null) {
                    blobLocation = uploadedBlobLocationsSupplier.apply(commitFile);
                    assert blobLocation != null : commitFile;
                    assert blobLocation.getBatchedCompoundCommitTermAndGeneration().before(primaryTermAndGeneration);
                }
                referencedFiles.put(commitFile, blobLocation);
            }
        }

        var replicatedContent = ReplicatedContent.create(useInternalFilesReplicatedContent, internalFiles, reference.getDirectory());
        var replicatedContentHeader = replicatedContent.header();

        var header = materializeCompoundCommitHeader(
            reference,
            internalFiles,
            replicatedContentHeader,
            referencedFiles,
            useInternalFilesReplicatedContent
        );

        if (logger.isDebugEnabled()) {
            var referencedBlobs = referencedFiles.values().stream().map(location -> location.blobFile().blobName()).distinct().count();
            logger.debug(
                "{}{} appending commit ({} bytes). References external {} files in {} other CCs and adds {} internal files {}.",
                shardId,
                primaryTermAndGeneration,
                header.length + replicatedContentHeader.dataSizeInBytes() + internalFilesSize,
                referencedFiles.size(),
                referencedBlobs,
                internalFiles.size(),
                internalFiles
            );
        }

        // Add padding to the previous CC if it exists
        if (pendingCompoundCommits.isEmpty() == false) {
            var lastCompoundCommit = pendingCompoundCommits.last();
            long lastCompoundCommitSize = lastCompoundCommit.getSizeInBytes();
            long lastCompoundCommitSizePageAligned = BlobCacheUtils.toPageAlignedSize(lastCompoundCommitSize);
            int padding = Math.toIntExact(lastCompoundCommitSizePageAligned - lastCompoundCommitSize);
            if (padding > 0) {
                lastCompoundCommit.setPadding(padding);
                long paddingOffset = currentOffset.get();
                var previousPaddingOffset = internalDataReadersByOffset.put(paddingOffset, new InternalPaddingReader(padding));
                assert previousPaddingOffset == null;
                currentOffset.set(paddingOffset + padding);
            }
        }

        final long headerOffset = currentOffset.get();
        assert headerOffset == BlobCacheUtils.toPageAlignedSize(headerOffset) : "header offset is not page-aligned: " + headerOffset;
        var previousHeaderOffset = internalDataReadersByOffset.put(headerOffset, new InternalHeaderReader(header));
        assert previousHeaderOffset == null;

        long replicatedContentOffset = headerOffset + header.length;
        for (var replicatedRangeReader : replicatedContent.readers()) {
            var previousReplicatedContent = internalDataReadersByOffset.put(replicatedContentOffset, replicatedRangeReader);
            assert previousReplicatedContent == null;
            replicatedContentOffset += replicatedRangeReader.rangeLength();
        }
        assert replicatedContentOffset == headerOffset + header.length + replicatedContentHeader.dataSizeInBytes();

        long internalFileOffset = headerOffset + header.length + replicatedContentHeader.dataSizeInBytes();

        // Map of all compound commit (CC) files with their internal or referenced blob location
        final var commitFiles = new HashMap<>(referencedFiles);

        for (var internalFile : internalFiles) {
            var fileLength = internalFile.length();
            var blobLocation = new BlobLocation(blobFile, internalFileOffset, fileLength);

            var previousFile = commitFiles.put(internalFile.name(), blobLocation);
            assert previousFile == null : internalFile.name();

            var previousLocation = internalLocations.put(internalFile.name(), blobLocation);
            assert previousLocation == null : internalFile.name();

            var previousOffset = internalDataReadersByOffset.put(
                internalFileOffset,
                new InternalFileReader(internalFile.name(), reference.getDirectory())
            );
            assert previousOffset == null : internalFile.name();
            internalFileOffset += fileLength;
        }
        currentOffset.set(internalFileOffset);

        var pendingCompoundCommit = new PendingCompoundCommit(
            header.length,
            reference,
            new StatelessCompoundCommit(
                shardId,
                ccTermAndGen,
                reference.getTranslogRecoveryStartFile(),
                nodeEphemeralId,
                Collections.unmodifiableMap(commitFiles),
                header.length + replicatedContentHeader.dataSizeInBytes() + internalFilesSize,
                internalFiles.stream().map(InternalFile::name).collect(Collectors.toUnmodifiableSet()),
                header.length,
                replicatedContent.header()
            )
        );
        pendingCompoundCommits.add(pendingCompoundCommit);
        assert currentOffset.get() == headerOffset + pendingCompoundCommit.getSizeInBytes()
            : "current offset "
                + currentOffset.get()
                + " should be equal to header offset "
                + headerOffset
                + " plus size of pending compound commit "
                + pendingCompoundCommit.getSizeInBytes();
        assert assertInternalConsistency();
        return true;
    }

    private boolean assertInternalConsistency() {
        final Set<String> allInternalFiles = pendingCompoundCommits.stream()
            .flatMap(pc -> pc.getStatelessCompoundCommit().internalFiles().stream())
            .collect(Collectors.toUnmodifiableSet());
        assert allInternalFiles.equals(internalLocations.keySet()) : "all internal files must have internal blobLocations";

        final var sizeInBytes = pendingCompoundCommits.stream().mapToLong(PendingCompoundCommit::getSizeInBytes).sum();
        assert sizeInBytes == currentOffset.get() : "current offset must be at the end of the VBCC";

        var it = pendingCompoundCommits.iterator();
        while (it.hasNext()) {
            var pendingCompoundCommit = it.next();
            // Assert that compound commits have padding to be page-aligned, except for the last compound commit
            assert it.hasNext() == false
                || pendingCompoundCommit.getSizeInBytes() == BlobCacheUtils.toPageAlignedSize(
                    pendingCompoundCommit.getStatelessCompoundCommit().sizeInBytes()
                )
                : "intermediate statelessCompoundCommit size in bytes "
                    + pendingCompoundCommit.getStatelessCompoundCommit().sizeInBytes()
                    + " plus padding length "
                    + pendingCompoundCommit.padding
                    + " should be equal to page-aligned size in bytes "
                    + BlobCacheUtils.toPageAlignedSize(pendingCompoundCommit.getStatelessCompoundCommit().sizeInBytes());
            assert it.hasNext() || pendingCompoundCommit.padding == 0 : "last pending compound commit should not have padding";

            // Assert that all generational files are contained in the same VBCC (no reference to a previous VBCC or BCC)
            for (var commitFile : pendingCompoundCommit.getStatelessCompoundCommit().commitFiles().entrySet()) {
                assert isGenerationalFile(commitFile.getKey()) == false
                    || commitFile.getValue().getBatchedCompoundCommitTermAndGeneration().equals(primaryTermAndGeneration)
                    : "generational file "
                        + commitFile.getValue()
                        + " should be located in BCC "
                        + primaryTermAndGeneration
                        + " but got "
                        + commitFile.getValue().getBatchedCompoundCommitTermAndGeneration();
            }
        }

        // Group the internal data readers by class
        final Map<Class<?>, List<InternalDataReader>> internalDataReaderGroups = internalDataReadersByOffset.values()
            .stream()
            .collect(groupingBy(internalHeaderOrFile -> internalHeaderOrFile.getClass()));
        assert internalDataReaderGroups.get(InternalHeaderReader.class).size() == pendingCompoundCommits.size()
            : "all pending CCs must have header offsets";
        assert allInternalFiles.equals(
            Set.copyOf(internalDataReaderGroups.get(InternalFileReader.class).stream().map(r -> ((InternalFileReader) r).filename).toList())
        ) : "all internal files must have offsets";
        if (internalDataReaderGroups.containsKey(InternalPaddingReader.class)) {
            assert internalDataReaderGroups.get(InternalPaddingReader.class).size() < pendingCompoundCommits.size()
                : "paddings "
                    + internalDataReaderGroups.get(InternalPaddingReader.class).size()
                    + " are more than pending CCs (excluding the last one) "
                    + (pendingCompoundCommits.size() - 1);
            internalDataReaderGroups.get(InternalPaddingReader.class).forEach(reader -> {
                assert reader instanceof InternalPaddingReader;
                InternalPaddingReader paddingReader = (InternalPaddingReader) reader;
                assert paddingReader.padding < SharedBytes.PAGE_SIZE
                    : "padding " + paddingReader.padding + " is more than page size " + SharedBytes.PAGE_SIZE;
            });
        }
        return true;
    }

    public BatchedCompoundCommit getFrozenBatchedCompoundCommit() {
        assert isFrozen() : "Cannot serialize before freeze";
        assert assertInternalConsistency();

        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>(pendingCompoundCommits.size());
        for (PendingCompoundCommit pendingCompoundCommit : pendingCompoundCommits) {
            compoundCommits.add(pendingCompoundCommit.getStatelessCompoundCommit());
        }
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    public InputStream getFrozenInputStreamForUpload() {
        assert isFrozen() : "Cannot stream before freeze";
        assert assertInternalConsistency();
        return getInputStreamForUpload();
    }

    InputStream getInputStreamForUpload() {
        mustIncRef();
        List<Long> offsets = internalDataReadersByOffset.navigableKeySet().stream().collect(Collectors.toUnmodifiableList());
        return new SlicedInputStream(offsets.size()) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                final var offset = offsets.get(slice);
                final var reader = internalDataReadersByOffset.get(offset);
                return reader.getInputStream(0L, Long.MAX_VALUE);
            }

            @Override
            public void close() throws IOException {
                if (isClosed() == false) {
                    try {
                        super.close();
                    } finally {
                        decRef();
                    }
                }
            }
        };
    }

    public String getBlobName() {
        return blobFile.blobName();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
        return primaryTermAndGeneration;
    }

    @Override
    public PrimaryTermAndGeneration primaryTermAndGeneration() {
        return getPrimaryTermAndGeneration();
    }

    public long getTotalSizeInBytes() {
        return currentOffset.get();
    }

    public Map<String, BlobLocation> getInternalLocations() {
        return internalLocations;
    }

    public long getCreationTimeInMillis() {
        return creationTimeInMillis;
    }

    public StatelessCompoundCommit lastCompoundCommit() {
        assert pendingCompoundCommits.isEmpty() == false;
        return pendingCompoundCommits.last().getStatelessCompoundCommit();
    }

    public long getMaxGeneration() {
        assert pendingCompoundCommits.isEmpty() == false;
        return pendingCompoundCommits.last().getGeneration();
    }

    public PendingCompoundCommit getLastPendingCompoundCommit() {
        return pendingCompoundCommits.last();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(pendingCompoundCommits);
    }

    List<PendingCompoundCommit> getPendingCompoundCommits() {
        return List.copyOf(pendingCompoundCommits);
    }

    int size() {
        return pendingCompoundCommits.size();
    }

    Set<PrimaryTermAndGeneration> getPendingCompoundCommitGenerations() {
        return pendingCompoundCommits.stream()
            .map(PendingCompoundCommit::getStatelessCompoundCommit)
            .map(StatelessCompoundCommit::primaryTermAndGeneration)
            .collect(Collectors.toSet());
    }

    private byte[] materializeCompoundCommitHeader(
        StatelessCommitRef reference,
        Iterable<InternalFile> internalFiles,
        InternalFilesReplicatedRanges replicatedRanges,
        Map<String, BlobLocation> referencedFiles,
        boolean useInternalFilesReplicatedContent
    ) throws IOException {
        assert getBlobName() != null;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            var positionTrackingOutputStreamStreamOutput = new PositionTrackingOutputStreamStreamOutput(os);
            StatelessCompoundCommit.writeXContentHeader(
                shardId,
                reference.getGeneration(),
                reference.getPrimaryTerm(),
                nodeEphemeralId,
                reference.getTranslogRecoveryStartFile(),
                referencedFiles,
                internalFiles,
                replicatedRanges,
                CURRENT_VERSION,
                positionTrackingOutputStreamStreamOutput,
                useInternalFilesReplicatedContent
            );
            return os.toByteArray();
        }
    }

    BlobLocation getBlobLocation(String fileName) {
        var internalLocation = internalLocations.get(fileName);
        return internalLocation == null ? uploadedBlobLocationsSupplier.apply(fileName) : internalLocation;
    }

    /**
     * Get the bytes of the virtual batched compound commit by reading the internal files (headers and internal files of pending
     * compound commits) in the given range.
     * @param offset the offset in the virtual batched compound commit to start reading internal files
     * @param length the length of the range to read
     * @param output the output to write the bytes to
     * @throws IOException
     */
    public void getBytesByRange(final long offset, final long length, final OutputStream output) throws IOException {
        assert offset >= 0;
        assert length >= 0 : "invalid length " + length;
        assert offset + length <= currentOffset.get() : "range [" + offset + ", " + length + "] more than " + currentOffset.get();
        assert ThreadPool.assertCurrentThreadPool(
            Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
            Stateless.SHARD_WRITE_THREAD_POOL,
            Stateless.PREWARM_THREAD_POOL
        );

        if (tryIncRef()) {
            try {
                NavigableMap<Long, InternalDataReader> subMap = internalDataReadersByOffset.subMap(
                    internalDataReadersByOffset.floorKey(offset),
                    true,
                    // could have been offset + length - 1, but we avoid an `if` that we'd
                    // otherwise need to avoid a NPE for the case of getBytesByRange(0, 0).
                    internalDataReadersByOffset.floorKey(offset + length),
                    true
                );
                long remainingBytesToRead = length;
                for (var entry : subMap.entrySet()) {
                    if (remainingBytesToRead <= 0) {
                        break;
                    }
                    InternalDataReader internalDataReader = entry.getValue();
                    long skipBytes = Math.max(0, offset - entry.getKey()); // can be non-zero only for the first entry
                    try (var inputStream = internalDataReader.getInputStream(skipBytes, remainingBytesToRead)) {
                        long bytesRead = Streams.copy(inputStream, output, false);
                        remainingBytesToRead -= bytesRead;
                    }
                }
                assert remainingBytesToRead == 0 : "remaining bytes to read " + remainingBytesToRead;
            } finally {
                decRef();
            }
        } else {
            throw buildResourceNotFoundException(shardId, primaryTermAndGeneration);
        }
    }

    @Override
    public String toString() {
        return "VirtualBatchedCompoundCommit{"
            + "shardId="
            + shardId
            + ", primaryTermAndGeneration="
            + primaryTermAndGeneration
            + ", size="
            + size()
            + ", nodeEphemeralId='"
            + nodeEphemeralId
            + '\''
            + ", creationTimeInMillis="
            + creationTimeInMillis
            + ", frozen="
            + frozen
            + '}';
    }

    public static ResourceNotFoundException buildResourceNotFoundException(
        ShardId shardId,
        PrimaryTermAndGeneration primaryTermAndGeneration
    ) {
        return new ResourceNotFoundException("BCC for shard " + shardId + " and " + primaryTermAndGeneration + " is already uploaded");
    }

    private boolean assertCompareAndSetFreezeOrAppendingCommitThread(Thread current, Thread updated) {
        final Thread witness = appendingCommitThread.compareAndExchange(current, updated);
        assert witness == current
            : "Unable to set appending commit thread to ["
                + updated
                + "]: expected thread ["
                + current
                + "] to be the appending commit thread, but thread "
                + witness
                + " is already appending a commit to "
                + getBlobName();
        return true;
    }

    static class PendingCompoundCommit implements Closeable, Comparable<PendingCompoundCommit> {
        private final int headerSize;
        private final StatelessCommitRef reference;
        private final StatelessCompoundCommit statelessCompoundCommit;
        // No need to be volatile because writing is synchronized at higher level in StatelessCommitService
        // and reading is dispatched to another thread after a second synchronization
        private int padding = 0;

        /**
         * Creates a new pending to upload compound commit. Note that the last pending compound commit should not have padding. The
         * padding is added to the previous pending compound commit when appending a new pending compound commit.
         * @param headerSize the size of materialized compound commit header
         * @param reference the lucene commit reference
         * @param statelessCompoundCommit the associated compound commit that will be uploaded
         */
        PendingCompoundCommit(int headerSize, StatelessCommitRef reference, StatelessCompoundCommit statelessCompoundCommit) {
            this.headerSize = headerSize;
            this.reference = reference;
            this.statelessCompoundCommit = statelessCompoundCommit;
        }

        void setPadding(int padding) {
            this.padding = padding;
            assert padding >= 0 : "padding " + padding + " is negative";
        }

        long getGeneration() {
            return reference.getGeneration();
        }

        StatelessCommitRef getCommitReference() {
            return reference;
        }

        /**
         * the size of the compound commit including codec, header, checksums, all files, and padding
         * Note that the last pending compound commit should not have padding. The padding is added to the previous pending compound commit
         * when appending a new pending compound commit.
         */
        public long getSizeInBytes() {
            return statelessCompoundCommit.sizeInBytes() + padding;
        }

        public StatelessCompoundCommit getStatelessCompoundCommit() {
            return statelessCompoundCommit;
        }

        // package-private for testing
        long getHeaderSize() {
            return headerSize;
        }

        @Override
        public int compareTo(PendingCompoundCommit o) {
            return Long.compare(getGeneration(), o.getGeneration());
        }

        @Override
        public void close() throws IOException {
            logger.debug(
                "{} releasing Lucene commit [term={}, gen={}]",
                statelessCompoundCommit.shardId(),
                reference.getPrimaryTerm(),
                reference.getGeneration()
            );
            reference.close();
        }
    }

    /**
     * Interface for reading internal data from a batched compound commit
     */
    @FunctionalInterface
    interface InternalDataReader {
        /**
         * Get the {@link InputStream} for reading the internal data.
         * @param offset the number of bytes to skip in the internal data before starting to read the internal data.
         * @param length the max number of bytes to read. ineffective if larger than the remaining available size of the internal data.
         */
        InputStream getInputStream(long offset, long length) throws IOException;
    }

    /**
     * Internal data reader for header bytes
     */
    private record InternalHeaderReader(byte[] header) implements InternalDataReader {
        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            final var stream = new ByteArrayStreamInput(header);
            stream.skipNBytes(offset);
            return limitStream(stream, length);
        }
    }

    /**
     * Internal data reader for an internal file
     */
    private record InternalFileReader(String filename, Directory directory) implements InternalDataReader {
        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            long fileLength = directory.fileLength(filename);
            assert offset < fileLength : "offset [" + offset + "] more than file length [" + fileLength + "]";
            long fileBytesToRead = Math.min(length, fileLength - offset);
            IndexInput input = directory.openInput(filename, IOContext.READONCE);
            try {
                input.seek(offset);
                return new InputStreamIndexInput(input, fileBytesToRead) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(super::close, input);
                    }
                };
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(input);
                throw e;
            }
        }
    }

    /**
     * Internal data reader for padding bytes
     */
    private record InternalPaddingReader(int padding) implements InternalDataReader {

        public InternalPaddingReader {
            assert padding <= SharedBytes.PAGE_SIZE : "padding " + padding + " is more than page size " + SharedBytes.PAGE_SIZE;
        }

        private static final byte[] PADDING_BYTES;

        static {
            byte[] padding = new byte[SharedBytes.PAGE_SIZE];
            Arrays.fill(padding, (byte) 0);
            PADDING_BYTES = padding;
        }

        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            assert offset < padding : "offset [" + offset + "] more than padding length [" + padding + "]";
            int paddingBytesToRead = BlobCacheUtils.toIntBytes(Math.min(length, padding - offset));
            return limitStream(new ByteArrayStreamInput(PADDING_BYTES), paddingBytesToRead);
        }
    }
}
