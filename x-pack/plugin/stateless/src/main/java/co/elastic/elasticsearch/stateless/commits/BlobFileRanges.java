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

import org.apache.lucene.codecs.CodecUtil;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_FOOTER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_HEADER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;
import static java.util.Collections.unmodifiableNavigableMap;

/**
 * Used to know the position from which to read a file in a blob.
 * <p>
 * This class provides a {@link #getPosition(long, int)} method that takes an absolute position in a blob that we want to read, and returns
 * the actual position to read (which may differ). In most cases the method will return a position in the blob where the file is stored in
 * its entirety. In case the blob has been optimized to store some ranges of bytes of the file (like the header and footer) in the first
 * region of the blob, and if the number of bytes to read does not exceed the length of the range, the {@link #getPosition(long, int)}
 * method will return the actual position within the first region that points to the range.
 */
public class BlobFileRanges {

    private final BlobLocation blobLocation;
    private final NavigableMap<Long, ReplicatedByteRange> replicatedRanges;

    public BlobFileRanges(BlobLocation blobLocation) {
        this(blobLocation, Collections.emptyNavigableMap());
    }

    private BlobFileRanges(BlobLocation blobLocation, NavigableMap<Long, ReplicatedByteRange> replicatedRanges) {
        this.blobLocation = Objects.requireNonNull(blobLocation);
        this.replicatedRanges = Objects.requireNonNull(replicatedRanges);
    }

    public BlobLocation blobLocation() {
        return blobLocation;
    }

    public String blobName() {
        return blobLocation.blobName();
    }

    public PrimaryTermAndGeneration getBatchedCompoundCommitTermAndGeneration() {
        return blobLocation.getBatchedCompoundCommitTermAndGeneration();
    }

    public long primaryTerm() {
        return blobLocation.primaryTerm();
    }

    public long fileOffset() {
        return blobLocation.offset();
    }

    public long fileLength() {
        return blobLocation.fileLength();
    }

    /**
     * Returns the actual position to read in the blob
     *
     * @param position  the position that we want to start reading from (absolute position from the beginning of the blob)
     * @param length    the length of bytes to read
     * @return          the actual position to start reading the blob from (which may differ from {@code position})
     */
    public long getPosition(long position, int length) {
        if (replicatedRanges.isEmpty() == false) {
            short len = (short) length;
            if (length == (int) len) {
                // greatest range that is less than or equal to the position to start reading from (or null if there is no such range)
                var candidate = replicatedRanges.floorEntry(position);
                if (candidate != null) {
                    return candidate.getValue().getPosition(position, len);
                }
            }
        }
        return position;
    }

    /**
     * Represents a range of {@code length} bytes that is originally stored at {@code position} in a blob and which is also copied at a
     * different {@code copy} position within the same blob.
     * Note: {@code position} and {@code copy} are absolute offsets starting from the beginning of the blob.
     *
     * @param position  the position at which the original range of bytes starts in the blob
     * @param length    the length of the range of bytes
     * @param copy      the position at which a copy of the same bytes exists in the blob
     */
    private record ReplicatedByteRange(long position, short length, long copy) {

        /**
         * Returns the position to read in the replicated range if the bytes to read are present in the range, otherwise returns {@code pos}
         */
        private long getPosition(long pos, short len) {
            if (this.position <= pos && pos + len <= this.position + this.length) {
                return this.copy + (pos - this.position);
            }
            return pos;
        }
    }

    /**
     * Computes the {@link BlobFileRanges} for a given set of internal files of a {@link BatchedCompoundCommit}
     *
     * @param batchedCompoundCommit the batched compound commit
     * @param files                 the set of files for which the blob file ranges must be computed
     * @param useReplicatedRanges   if the replication of ranges feature is enabled
     * @return map of all the commit files of the last commit of the batched compound commit associated to their {@link BlobFileRanges}
     */
    public static Map<String, BlobFileRanges> computeBlobFileRanges(
        BatchedCompoundCommit batchedCompoundCommit,
        Set<String> files,
        boolean useReplicatedRanges
    ) {
        long blobOffset = 0L;
        var blobFileRanges = HashMap.<String, BlobFileRanges>newHashMap(files.size());
        for (var compoundCommit : batchedCompoundCommit.compoundCommits()) {
            assert blobOffset == BlobCacheUtils.toPageAlignedSize(blobOffset);
            var internalFiles = Sets.intersection(compoundCommit.internalFiles(), files);
            if (internalFiles.isEmpty() == false) {
                assert internalFiles.stream().noneMatch(blobFileRanges::containsKey);
                blobFileRanges.putAll(computeBlobFileRanges(useReplicatedRanges, compoundCommit, blobOffset, internalFiles));
            }
            blobOffset += BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
        }
        assert files.stream().allMatch(blobFileRanges::containsKey);
        return blobFileRanges;
    }

    /**
     * Computes the {@link BlobFileRanges} for a given set of internal files of a {@link StatelessCompoundCommit}
     * Currently only used by the tests.
     */
    static Map<String, BlobFileRanges> computeBlobFileRanges(
        boolean useReplicatedRanges,
        StatelessCompoundCommit compoundCommit,
        long blobOffset,
        Set<String> internalFiles
    ) {
        long replicatedRangesOffset = blobOffset + compoundCommit.headerSizeInBytes();
        long internalFilesOffset = replicatedRangesOffset + compoundCommit.internalFilesReplicatedRanges().dataSizeInBytes();

        var replicatedRanges = new TreeMap<Long, ReplicatedByteRange>();
        for (var range : compoundCommit.internalFilesReplicatedRanges().replicatedRanges()) {
            long position = internalFilesOffset + range.position();
            var previous = replicatedRanges.put(position, new ReplicatedByteRange(position, range.length(), replicatedRangesOffset));
            assert previous == null : "replicated range already exists: " + previous;
            replicatedRangesOffset += range.length();
        }
        assert assertReplicatedRangesMatchInternalFiles(compoundCommit, replicatedRanges);
        assert assertNoOverlappingReplicatedRanges(replicatedRanges);

        var blobFileRanges = HashMap.<String, BlobFileRanges>newHashMap(internalFiles.size());
        for (var internalFile : internalFiles) {
            var blobLocation = compoundCommit.commitFiles().get(internalFile);
            assert blobLocation != null : internalFile;
            var floor = replicatedRanges.floorEntry(blobLocation.offset());
            if (useReplicatedRanges && floor != null) {
                blobFileRanges.put(
                    internalFile,
                    // tailMap returns a view of the backing map where the first element is the replicated range corresponding to
                    // the file's header, followed by the replicated range for the file's footer, and then replicated ranges for
                    // other files.
                    new BlobFileRanges(blobLocation, unmodifiableNavigableMap(replicatedRanges.tailMap(floor.getKey(), true)))
                );
            } else {
                blobFileRanges.put(internalFile, new BlobFileRanges(blobLocation));
            }
        }
        return blobFileRanges;
    }

    /**
     * Asserts that replicated ranges exist for header/footer of compound commit internal files.
     */
    private static boolean assertReplicatedRangesMatchInternalFiles(
        StatelessCompoundCommit commit,
        TreeMap<Long, ReplicatedByteRange> ranges
    ) {
        if (commit.internalFilesReplicatedRanges().isEmpty() == false) {
            for (var internalFile : commit.internalFiles()) {
                var blobLocation = commit.commitFiles().get(internalFile);
                assert blobLocation != null : internalFile;

                var header = ranges.floorEntry(blobLocation.offset());
                assert header != null : "header not found for " + internalFile + " at position " + blobLocation.offset();
                assert header.getValue() != null;
                assert header.getValue().length >= ((blobLocation.fileLength() <= REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE)
                    ? blobLocation.fileLength()
                    : REPLICATED_CONTENT_HEADER_SIZE);

                long footerPosition = blobLocation.offset() + blobLocation.fileLength() - CodecUtil.footerLength();
                var footer = ranges.floorEntry(footerPosition);
                assert footer != null : "footer not found for " + internalFile + " at position " + footerPosition;
                assert footer.getValue() != null;
                assert footer.getValue().length >= REPLICATED_CONTENT_FOOTER_SIZE;
            }
        }
        return true;
    }

    private static boolean assertNoOverlappingReplicatedRanges(TreeMap<Long, ReplicatedByteRange> ranges) {
        ReplicatedByteRange previous = null;
        for (var range : ranges.entrySet()) {
            assert previous == null || previous.copy + previous.length <= range.getValue().copy : previous + " vs " + range;
            previous = range.getValue();
        }
        return true;
    }
}
