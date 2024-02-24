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
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final ShardId shardId;
    private final String nodeEphemeralId;
    private final Function<String, BlobLocation> uploadedBlobLocationsSupplier;
    private final NavigableSet<PendingCompoundCommit> pendingCompoundCommits;
    // TODO: the internal files should be added to the corresponding BlobReferences
    private final Map<String, BlobLocation> internalLocations = new ConcurrentHashMap<>();
    private final AtomicLong currentOffset = new AtomicLong();
    private final String blobName;
    private final AtomicReference<Thread> appendingCommitThread = new AtomicReference<>();
    private final PrimaryTermAndGeneration primaryTermAndGeneration;

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

    public void appendCommit(StatelessCommitRef reference) throws IOException {
        assert assertCompareAndSetAppendingCommitThread(null, Thread.currentThread());
        try {
            doAppendCommit(reference);
        } finally {
            assert assertCompareAndSetAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    private void doAppendCommit(StatelessCommitRef reference) throws IOException {
        assert primaryTermAndGeneration.primaryTerm() == reference.getPrimaryTerm();
        assert primaryTermAndGeneration.generation() <= reference.getGeneration();
        assert pendingCompoundCommits.isEmpty() || pendingCompoundCommits.last().getGeneration() < reference.getGeneration();
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.FLUSH, ThreadPool.Names.REFRESH);
        // TODO: add #freeze method to ensure that no new commits are added after a new VBCC is created

        // TODO: align 4KiB
        var internalFiles = computeInternalFiles(reference);
        long compoundCommitFilesSize = internalFiles.stream().mapToLong(StatelessCompoundCommit.InternalFile::length).sum();
        var header = materializeCompoundCommitHeader(reference, internalFiles);

        long startingOffset = currentOffset.get() + header.length;
        // TODO: get rid of the blob length
        long blobLength = startingOffset + compoundCommitFilesSize;
        long internalFileOffset = startingOffset;
        for (var internalFile : internalFiles) {
            var fileLength = internalFile.length();
            var previousLocation = internalLocations.put(
                internalFile.name(),
                new BlobLocation(primaryTermAndGeneration.primaryTerm(), blobName, blobLength, internalFileOffset, fileLength)
            );
            assert previousLocation == null;
            internalFileOffset += fileLength;
        }

        currentOffset.set(internalFileOffset);

        var pendingCompoundCommit = new PendingCompoundCommit(header, header.length + compoundCommitFilesSize, reference, internalFiles);
        pendingCompoundCommits.add(pendingCompoundCommit);
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
        // Ensure that we get stable ordering until we get ES-7665 implemented
        Collections.sort(internalFiles);
        return Collections.unmodifiableList(internalFiles);
    }

    public BatchedCompoundCommit writeToStore(OutputStream output) throws IOException {
        for (PendingCompoundCommit compoundCommit : pendingCompoundCommits) {
            compoundCommit.writeToStore(output);
        }

        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>(pendingCompoundCommits.size());
        for (PendingCompoundCommit pendingCompoundCommit : pendingCompoundCommits) {
            var commitRef = pendingCompoundCommit.getCommitReference();

            Map<String, BlobLocation> commitLocations = Maps.newMapWithExpectedSize(commitRef.getCommitFiles().size());
            for (String commitFile : commitRef.getCommitFiles()) {
                var blobLocation = getBlobLocation(commitFile);
                assert blobLocation != null;
                commitLocations.put(commitFile, blobLocation);
            }
            compoundCommits.add(
                new StatelessCompoundCommit(
                    shardId,
                    commitRef.getGeneration(),
                    commitRef.getPrimaryTerm(),
                    nodeEphemeralId,
                    Collections.unmodifiableMap(commitLocations),
                    pendingCompoundCommit.getSizeInBytes()
                )
            );
        }
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    public String getBlobName() {
        return blobName;
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(pendingCompoundCommits);
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

    private BlobLocation getBlobLocation(String fileName) {
        var internalLocation = internalLocations.get(fileName);
        return internalLocation == null ? uploadedBlobLocationsSupplier.apply(fileName) : internalLocation;
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
        private final long sizeInBytes;
        private final StatelessCommitRef reference;

        /**
         * Creates a new pending to upload compound commit
         * @param header the materialized compound commit header
         * @param sizeInBytes the size of the compound commit including codec, header, checksums and all files
         * @param reference the lucene commit reference
         */
        PendingCompoundCommit(
            byte[] header,
            long sizeInBytes,
            StatelessCommitRef reference,
            List<StatelessCompoundCommit.InternalFile> internalFiles
        ) {
            this.sizeInBytes = sizeInBytes;
            this.reference = reference;
            this.header = header;
            this.internalFiles = internalFiles;
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

        public long getSizeInBytes() {
            return sizeInBytes;
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
}
