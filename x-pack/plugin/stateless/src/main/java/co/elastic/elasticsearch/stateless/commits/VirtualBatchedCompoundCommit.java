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
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

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
 *     {@link #writeToStore(OutputStream)} method, the caller should promptly invoke {@link #close()} on the instance.
 *     This action releases the acquired Lucene commit reference and facilitates the proper release of associated resources.
 * </p>
 *
 * <p>
 *     This class facilitates the appending of multiple compound commits via {@link #appendCommit(StatelessCommitRef)}.
 *     When the caller intends to write these commits to the blob store it should use {@link #writeToStore(OutputStream)}.
 *  </p>
 *
 * */
public class VirtualBatchedCompoundCommit extends AbstractRefCounted implements Closeable {
    private static final Logger logger = LogManager.getLogger(VirtualBatchedCompoundCommit.class);

    private final ShardId shardId;
    private final String nodeEphemeralId;
    private final Function<String, BlobLocation> uploadedBlobLocationsSupplier;
    private final NavigableSet<PendingCompoundCommit> pendingCompoundCommits;
    // TODO: the internal files should be added to the corresponding BlobReferences
    private final Map<String, BlobLocation> internalLocations = new ConcurrentHashMap<>();
    // Maps internal files (pending compound commits' headers and internal files) to their offset in the virtual batched compound commit
    private final NavigableMap<Long, InternalHeaderOrFile> internalFilesByOffset = new ConcurrentSkipListMap<>();
    private final AtomicLong currentOffset = new AtomicLong();
    private final String blobName;
    private final AtomicReference<Thread> appendingCommitThread = new AtomicReference<>();
    private final PrimaryTermAndGeneration primaryTermAndGeneration;
    // VBCC can no longer be appended to once it is frozen
    private volatile boolean frozen = false;

    public VirtualBatchedCompoundCommit(
        ShardId shardId,
        String nodeEphemeralId,
        long primaryTerm,
        long generation,
        Function<String, BlobLocation> uploadedBlobLocationsSupplier
    ) {
        this.shardId = shardId;
        this.nodeEphemeralId = nodeEphemeralId;
        this.uploadedBlobLocationsSupplier = uploadedBlobLocationsSupplier;
        this.pendingCompoundCommits = new ConcurrentSkipListSet<>();
        this.primaryTermAndGeneration = new PrimaryTermAndGeneration(primaryTerm, generation);
        this.blobName = StatelessCompoundCommit.blobNameFromGeneration(generation);
    }

    /**
     * Freeze the VBCC so that no more CC can be appended. The VBCC is guaranteed to be frozen afterwards.
     * @return {@code true} if the VBCC is frozen by this thread or
     * {@code false} if it is already frozen or concurrently frozen by other threads.
     */
    public boolean freeze() {
        assert pendingCompoundCommits.isEmpty() == false : "Cannot freeze an empty virtual batch compound commit";

        if (isFrozen()) {
            return false;
        }
        synchronized (this) {
            if (isFrozen()) {
                return false;
            }
            frozen = true;
            logger.debug("VBCC is successfully frozen");
            return true;
        }
    }

    public boolean appendCommit(StatelessCommitRef reference) {
        assert assertCompareAndSetAppendingCommitThread(null, Thread.currentThread());
        try {
            return doAppendCommit(reference);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to append commit [" + reference.getPrimaryTerm() + ", " + reference.getGeneration() + "]",
                e
            );
        } finally {
            assert assertCompareAndSetAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    // package private for testing
    boolean isFrozen() {
        return frozen;
    }

    private boolean doAppendCommit(StatelessCommitRef reference) throws IOException {
        assert primaryTermAndGeneration.primaryTerm() == reference.getPrimaryTerm();
        assert (pendingCompoundCommits.isEmpty() && primaryTermAndGeneration.generation() == reference.getGeneration())
            || (pendingCompoundCommits.isEmpty() == false && primaryTermAndGeneration.generation() < reference.getGeneration());
        assert pendingCompoundCommits.isEmpty() || pendingCompoundCommits.last().getGeneration() < reference.getGeneration();
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.FLUSH, ThreadPool.Names.REFRESH, Stateless.SHARD_WRITE_THREAD_POOL);

        // bail early if VBCC is already frozen to avoid doing any work
        if (isFrozen()) {
            return false;
        }

        // TODO: align 4KiB
        var internalFiles = computeInternalFiles(reference);
        long compoundCommitFilesSize = internalFiles.stream().mapToLong(StatelessCompoundCommit.InternalFile::length).sum();
        var header = materializeCompoundCommitHeader(reference, internalFiles);

        // Blocking when adding the new CC and updating relevant fields so that they offer consistent view to other (freezing) threads
        synchronized (this) {
            if (isFrozen()) {
                return false;
            }

            final long headerOffset = currentOffset.get();
            long startingOffset = headerOffset + header.length;
            // TODO: get rid of the blob length
            long blobLength = startingOffset + compoundCommitFilesSize;
            long internalFileOffset = startingOffset;
            long[] offsets = new long[internalFiles.size()];
            for (int i = 0; i < internalFiles.size(); i++) {
                var internalFile = internalFiles.get(i);
                offsets[i] = internalFileOffset;
                var fileLength = internalFile.length();
                var previousLocation = internalLocations.put(
                    internalFile.name(),
                    new BlobLocation(primaryTermAndGeneration.primaryTerm(), blobName, blobLength, internalFileOffset, fileLength)
                );
                assert previousLocation == null;
                internalFileOffset += fileLength;
            }
            currentOffset.set(internalFileOffset);

            var pendingCompoundCommit = new PendingCompoundCommit(
                header,
                reference,
                internalFiles,
                createStatelessCompoundCommit(reference, header.length + compoundCommitFilesSize)
            );
            pendingCompoundCommits.add(pendingCompoundCommit);
            logger.debug("appended new CC [{}] to VBCC [{}]", pendingCompoundCommit, primaryTermAndGeneration);

            internalFilesByOffset.put(headerOffset, new InternalHeaderOrFile(pendingCompoundCommit, null));
            for (int i = 0; i < internalFiles.size(); i++) {
                var internalFile = internalFiles.get(i);
                var previousOffset = internalFilesByOffset.put(
                    offsets[i],
                    new InternalHeaderOrFile(pendingCompoundCommit, internalFile.name())
                );
                assert previousOffset == null;
            }
        }
        // The consistency can be asserted outside the blocking code since appendCommit runs single-threaded
        assert assertInternalConsistency();
        return true;
    }

    private StatelessCompoundCommit createStatelessCompoundCommit(StatelessCommitRef reference, long sizeInBytes) {
        Map<String, BlobLocation> commitLocations = Maps.newMapWithExpectedSize(reference.getCommitFiles().size());
        for (String commitFile : reference.getCommitFiles()) {
            var blobLocation = getBlobLocation(commitFile);
            assert blobLocation != null;
            commitLocations.put(commitFile, blobLocation);
        }
        return new StatelessCompoundCommit(
            reference.getShardId(),
            new PrimaryTermAndGeneration(reference.getPrimaryTerm(), reference.getGeneration()),
            reference.getTranslogRecoveryStartFile(),
            nodeEphemeralId,
            Collections.unmodifiableMap(commitLocations),
            sizeInBytes
        );
    }

    private boolean assertInternalConsistency() {
        final Set<String> allInternalFiles = pendingCompoundCommits.stream()
            .flatMap(pc -> pc.internalFiles.stream())
            .map(StatelessCompoundCommit.InternalFile::name)
            .collect(Collectors.toUnmodifiableSet());
        assert allInternalFiles.equals(internalLocations.keySet()) : "all internal files must have internal blobLocations";

        final var sizeInBytes = pendingCompoundCommits.stream().mapToLong(PendingCompoundCommit::getSizeInBytes).sum();
        assert sizeInBytes == currentOffset.get() : "current offset must be at the end of the VBCC";
        final Map<Boolean, List<String>> internalFileGroup = internalFilesByOffset.values()
            .stream()
            .collect(
                groupingBy(
                    internalHeaderOrFile -> internalHeaderOrFile.internalFile == null,
                    Collectors.mapping(pc -> pc.internalFile, Collectors.toList())
                )
            );
        assert internalFileGroup.get(true).size() == pendingCompoundCommits.size() : "all pending CCs must have header offsets";
        assert allInternalFiles.equals(Set.copyOf(internalFileGroup.get(false))) : "all internal files must have offsets";
        return true;
    }

    private List<StatelessCompoundCommit.InternalFile> computeInternalFiles(StatelessCommitRef commitRef) throws IOException {
        var additionalFiles = commitRef.getAdditionalFiles();
        List<StatelessCompoundCommit.InternalFile> internalFiles = new ArrayList<>();
        for (String commitFile : commitRef.getCommitFiles()) {
            if (additionalFiles.contains(commitFile)
                || (StatelessCommitService.isGenerationalFile(commitFile) && internalLocations.containsKey(commitFile) == false)) {
                internalFiles.add(new StatelessCompoundCommit.InternalFile(commitFile, commitRef.getDirectory().fileLength(commitFile)));
            }
        }
        Collections.sort(internalFiles);
        return Collections.unmodifiableList(internalFiles);
    }

    public BatchedCompoundCommit writeToStore(OutputStream output) throws IOException {
        assert isFrozen() : "Cannot serialize before freeze";
        assert assertInternalConsistency();

        for (PendingCompoundCommit compoundCommit : pendingCompoundCommits) {
            compoundCommit.writeToStore(output);
        }

        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>(pendingCompoundCommits.size());
        for (PendingCompoundCommit pendingCompoundCommit : pendingCompoundCommits) {
            compoundCommits.add(pendingCompoundCommit.getStatelessCompoundCommit());
        }
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    public String getBlobName() {
        return blobName;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getGeneration() {
        return primaryTermAndGeneration.generation();
    }

    public long getTotalSizeInBytes() {
        return pendingCompoundCommits.stream().mapToLong(PendingCompoundCommit::getSizeInBytes).sum();
    }

    public Set<String> getInternalFiles() {
        return internalLocations.keySet();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(pendingCompoundCommits);
    }

    // package private for testing
    List<PendingCompoundCommit> getPendingCompoundCommits() {
        return List.copyOf(pendingCompoundCommits);
    }

    private byte[] materializeCompoundCommitHeader(StatelessCommitRef reference, List<StatelessCompoundCommit.InternalFile> internalFiles)
        throws IOException {
        assert getBlobName() != null;

        var internalFileNames = internalFiles.stream().map(StatelessCompoundCommit.InternalFile::name).collect(Collectors.toSet());
        Map<String, BlobLocation> commitFiles = Maps.newMapWithExpectedSize(reference.getCommitFiles().size());
        for (String fileName : reference.getCommitFiles()) {
            if (internalFileNames.contains(fileName) == false) {
                var location = getBlobLocation(fileName);
                commitFiles.put(fileName, location);
            }
        }

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            var positionTrackingOutputStreamStreamOutput = new PositionTrackingOutputStreamStreamOutput(os);
            StatelessCompoundCommit.writeHeader(
                positionTrackingOutputStreamStreamOutput,
                shardId,
                reference.getGeneration(),
                reference.getPrimaryTerm(),
                nodeEphemeralId,
                reference.getTranslogRecoveryStartFile(),
                commitFiles,
                internalFiles
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
        assert ThreadPool.assertCurrentThreadPool(Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL);

        if (tryIncRef()) {
            try {
                NavigableMap<Long, InternalHeaderOrFile> subMap = internalFilesByOffset.subMap(
                    internalFilesByOffset.floorKey(offset),
                    true,
                    internalFilesByOffset.floorKey(offset + length), // could have been offset + length - 1, but we avoid an `if` that we'd
                                                                     // otherwise need to avoid a NPE for the case of getBytesByRange(0, 0).
                    true
                );
                long remainingBytesToRead = length;
                for (var entry : subMap.entrySet()) {
                    if (remainingBytesToRead <= 0) {
                        break;
                    }
                    InternalHeaderOrFile internalHeaderOrFile = entry.getValue();
                    long skipBytes = Math.max(0, offset - entry.getKey()); // can be non-zero only for the first entry
                    if (internalHeaderOrFile.internalFile() == null) {
                        byte[] header = internalHeaderOrFile.pendingCompoundCommit.header;
                        assert skipBytes < header.length : "skipBytes [" + skipBytes + "] more than header length [" + header.length + "]";
                        int headerBytesToRead = (int) Math.min(remainingBytesToRead, header.length - skipBytes);
                        output.write(header, (int) skipBytes, headerBytesToRead);
                        remainingBytesToRead -= headerBytesToRead;
                    } else {
                        var directory = internalHeaderOrFile.pendingCompoundCommit.getCommitReference().getDirectory();
                        var internalFile = internalHeaderOrFile.internalFile;
                        long fileLength = directory.fileLength(internalFile);
                        assert skipBytes < fileLength : "skipBytes [" + skipBytes + "] more than file length [" + fileLength + "]";
                        long fileBytesToRead = Math.min(remainingBytesToRead, fileLength - skipBytes);
                        try (IndexInput input = directory.openInput(internalFile, IOContext.READONCE)) {
                            input.seek(skipBytes);
                            Streams.copy(new InputStreamIndexInput(input, fileBytesToRead), output, false);
                        }
                        remainingBytesToRead -= fileBytesToRead;
                    }
                }
                assert remainingBytesToRead == 0 : "remaining bytes to read " + remainingBytesToRead;
            } finally {
                decRef();
            }
        } else {
            throw new BatchedCompoundCommitAlreadyUploaded(shardId, primaryTermAndGeneration);
        }
    }

    private boolean assertCompareAndSetAppendingCommitThread(Thread current, Thread updated) {
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
        private final byte[] header;
        private final List<StatelessCompoundCommit.InternalFile> internalFiles;
        private final StatelessCommitRef reference;
        private final StatelessCompoundCommit statelessCompoundCommit;

        /**
         * Creates a new pending to upload compound commit
         * @param header the materialized compound commit header
         * @param reference the lucene commit reference
         * @param statelessCompoundCommit the associated compound commit that will be uploaded
         */
        PendingCompoundCommit(
            byte[] header,
            StatelessCommitRef reference,
            List<StatelessCompoundCommit.InternalFile> internalFiles,
            StatelessCompoundCommit statelessCompoundCommit
        ) {
            this.reference = reference;
            this.header = header;
            this.internalFiles = internalFiles;
            this.statelessCompoundCommit = statelessCompoundCommit;
        }

        void writeToStore(OutputStream output) throws IOException {
            output.write(header);
            StatelessCompoundCommit.writeInternalFilesToStore(output, internalFiles, reference.getDirectory());
        }

        long getGeneration() {
            return reference.getGeneration();
        }

        StatelessCommitRef getCommitReference() {
            return reference;
        }

        /**
         * the size of the compound commit including codec, header, checksums and all files
         */
        public long getSizeInBytes() {
            return statelessCompoundCommit.sizeInBytes();
        }

        public StatelessCompoundCommit getStatelessCompoundCommit() {
            return statelessCompoundCommit;
        }

        // package-private for testing
        long getHeaderSize() {
            return header.length;
        }

        @Override
        public int compareTo(PendingCompoundCommit o) {
            return Long.compare(getGeneration(), o.getGeneration());
        }

        @Override
        public void close() throws IOException {
            reference.close();
        }
    }

    /**
     * Helper record to associate a requested offset in the {@link VirtualBatchedCompoundCommit} with an internal header or file.
     * @param pendingCompoundCommit the pending compound commit that contains the requested offset
     * @param internalFile the internal file, that the offset belongs to, or null if the offset is the
     *                     header of the {@link PendingCompoundCommit}
     */
    record InternalHeaderOrFile(PendingCompoundCommit pendingCompoundCommit, @Nullable String internalFile) {}

    public static class BatchedCompoundCommitAlreadyUploaded extends ElasticsearchException {
        public BatchedCompoundCommitAlreadyUploaded(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {
            super("batched compound commit for shard " + shardId + " and " + primaryTermAndGeneration + " is already uploaded");
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            // stack trace is uninteresting, as the exception simply signifies that the search shard should look into the object store
            return this;
        }
    }
}
