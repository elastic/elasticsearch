/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

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
     * Computes the {@link BlobFileRanges} for a given set of internal files of a {@link StatelessCompoundCommit}
     */
    public static Map<String, BlobFileRanges> computeBlobFileRanges(
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
        assert assertNoOverlappingReplicatedRanges(replicatedRanges);

        var blobFileRanges = HashMap.<String, BlobFileRanges>newHashMap(internalFiles.size());
        for (var internalFile : internalFiles) {
            var blobLocation = compoundCommit.commitFiles().get(internalFile);
            assert blobLocation != null : internalFile;
            if (useReplicatedRanges == false || replicatedRanges.isEmpty()) {
                blobFileRanges.put(internalFile, new BlobFileRanges(blobLocation));
                continue;
            }

            var header = replicatedRanges.floorKey(blobLocation.offset());
            var footer = replicatedRanges.floorKey(blobLocation.offset() + blobLocation.fileLength() - 1);
            if (header == null || footer == null) {
                blobFileRanges.put(internalFile, new BlobFileRanges(blobLocation));
                continue;
            }

            blobFileRanges.put(
                internalFile,
                new BlobFileRanges(blobLocation, unmodifiableNavigableMap(replicatedRanges.subMap(header, true, footer, true)))
            );
        }
        return blobFileRanges;
    }

    private static boolean assertNoOverlappingReplicatedRanges(TreeMap<Long, ReplicatedByteRange> ranges) {
        ReplicatedByteRange previous = null;
        for (var range : ranges.entrySet()) {
            assert previous == null || previous.copy + previous.length <= range.getValue().copy : previous + " vs " + range;
            previous = range.getValue();
        }
        return true;
    }

    public String toString() {
        return blobLocation.toString();
    }

}
