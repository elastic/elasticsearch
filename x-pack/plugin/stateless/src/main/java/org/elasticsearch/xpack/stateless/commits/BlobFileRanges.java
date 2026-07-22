/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

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
public class BlobFileRanges implements Writeable {

    private final BlobLocation blobLocation;
    private final NavigableMap<Long, ReplicatedByteRange> replicatedRanges;
    /**
     * The document timestamp range of the compound commit that produced this file, or {@code null} if unknown
     * (e.g. no {@code @timestamp} field, or this instance was constructed without a commit context).
     */
    @Nullable
    private final StatelessCompoundCommit.TimestampFieldValueRange timestampRange;

    public BlobFileRanges(BlobLocation blobLocation) {
        this(blobLocation, Collections.emptyNavigableMap(), null);
    }

    public BlobFileRanges(BlobLocation blobLocation, @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampRange) {
        this(blobLocation, Collections.emptyNavigableMap(), timestampRange);
    }

    private BlobFileRanges(
        BlobLocation blobLocation,
        NavigableMap<Long, ReplicatedByteRange> replicatedRanges,
        @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampRange
    ) {
        this.blobLocation = Objects.requireNonNull(blobLocation);
        this.replicatedRanges = Objects.requireNonNull(replicatedRanges);
        this.timestampRange = timestampRange;
    }

    public BlobFileRanges(final StreamInput in) throws IOException {
        blobLocation = BlobLocation.readFromTransport(in);
        replicatedRanges = unmodifiableNavigableMap(
            in.<Long, ReplicatedByteRange, TreeMap<Long, ReplicatedByteRange>>readMapValues(
                ReplicatedByteRange::new,
                ReplicatedByteRange::position,
                ignored -> new TreeMap<>()
            )
        );
        timestampRange = in.readOptionalWriteable(StatelessCompoundCommit.TimestampFieldValueRange::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        blobLocation.writeTo(out);
        out.writeMapValues(replicatedRanges);
        out.writeOptionalWriteable(timestampRange);
    }

    public BlobLocation blobLocation() {
        return blobLocation;
    }

    @Nullable
    public StatelessCompoundCommit.TimestampFieldValueRange timestampRange() {
        return timestampRange;
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
    private record ReplicatedByteRange(long position, short length, long copy) implements Writeable {

        /**
         * Returns the position to read in the replicated range if the bytes to read are present in the range, otherwise returns {@code pos}
         */
        private long getPosition(long pos, short len) {
            if (this.position <= pos && pos + len <= this.position + this.length) {
                return this.copy + (pos - this.position);
            }
            return pos;
        }

        ReplicatedByteRange(StreamInput in) throws IOException {
            this(in.readVLong(), in.readShort(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(position);
            out.writeShort(length);
            out.writeVLong(copy);
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
            long position = Math.addExact(internalFilesOffset, range.position());
            var previous = replicatedRanges.put(position, new ReplicatedByteRange(position, range.length(), replicatedRangesOffset));
            assert previous == null : "replicated range already exists: " + previous;
            replicatedRangesOffset += range.length();
        }
        assert assertNoOverlappingReplicatedRanges(replicatedRanges);

        final var timestampRange = compoundCommit.getTimestampFieldValueRange();
        var blobFileRanges = HashMap.<String, BlobFileRanges>newHashMap(internalFiles.size());
        for (var internalFile : internalFiles) {
            var blobLocation = compoundCommit.commitFiles().get(internalFile);
            assert blobLocation != null : internalFile;
            if (useReplicatedRanges == false || replicatedRanges.isEmpty()) {
                blobFileRanges.put(internalFile, new BlobFileRanges(blobLocation, Collections.emptyNavigableMap(), timestampRange));
                continue;
            }

            var header = replicatedRanges.floorKey(blobLocation.offset());
            var footer = replicatedRanges.floorKey(blobLocation.offset() + blobLocation.fileLength() - 1);
            if (header == null || footer == null) {
                blobFileRanges.put(internalFile, new BlobFileRanges(blobLocation, Collections.emptyNavigableMap(), timestampRange));
                continue;
            }

            blobFileRanges.put(
                internalFile,
                new BlobFileRanges(
                    blobLocation,
                    unmodifiableNavigableMap(replicatedRanges.subMap(header, true, footer, true)),
                    timestampRange
                )
            );
        }
        return blobFileRanges;
    }

    /**
     * Returns a positive epoch millis timestamp representing the midpoint of the given range, or
     * {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} if the range is null.
     */
    public static long midpointMillisOrUnknownForCache(@Nullable StatelessCompoundCommit.TimestampFieldValueRange range) {
        if (range == null) {
            return SharedBlobCacheService.UNKNOWN_TIMESTAMP;
        }
        long midpointMillis = range.midpointMillis();
        // Cache-region timestamps must be positive epoch millis or a negative sentinel value.
        return midpointMillis > 0L ? midpointMillis : 1L;
    }

    /**
     * Returns the most recent known timestamp between two timestamps, treating {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} as the
     * lesser-known value so any known timestamp wins.
     */
    public static long mostRecentKnownTimestamp(long a, long b) {
        if (a == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
            return b;
        }
        if (b == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
            return a;
        }
        return Math.max(a, b);
    }

    public boolean hasReplicatedRanges() {
        return replicatedRanges.isEmpty() == false;
    }

    /**
     * Reconciles this instance with {@code other}, both sharing the same blob location. The result uses the most recent timestamp
     * of the two (treating {@code null} as unknown), and the replicated ranges from whichever has them. If both have replicated ranges,
     * asserts they are identical.
     */
    public BlobFileRanges reconcileWith(final BlobFileRanges other) {
        assert blobLocation.equals(other.blobLocation)
            : "Cannot reconcile BlobFileRanges with different blob locations: " + blobLocation + " vs " + other.blobLocation;

        final NavigableMap<Long, ReplicatedByteRange> reconciledReplicatedRanges = reconcileReplicatedRanges(other);
        final StatelessCompoundCommit.TimestampFieldValueRange reconcileTimestampRange = reconcileTimestampRange(other);
        if (Objects.equals(replicatedRanges, reconciledReplicatedRanges) && Objects.equals(timestampRange, reconcileTimestampRange)) {
            return this;
        }
        return new BlobFileRanges(blobLocation, reconciledReplicatedRanges, reconcileTimestampRange);
    }

    private StatelessCompoundCommit.TimestampFieldValueRange reconcileTimestampRange(final BlobFileRanges other) {
        final var midpoint = midpointMillisOrUnknownForCache(timestampRange);
        final var otherMidpoint = midpointMillisOrUnknownForCache(other.timestampRange);
        return mostRecentKnownTimestamp(midpoint, otherMidpoint) == midpoint ? timestampRange : other.timestampRange;
    }

    private NavigableMap<Long, ReplicatedByteRange> reconcileReplicatedRanges(final BlobFileRanges other) {
        if (replicatedRanges.isEmpty()) {
            return other.replicatedRanges;
        }
        if (other.replicatedRanges.isEmpty()) {
            return replicatedRanges;
        }
        assert replicatedRanges.equals(other.replicatedRanges)
            : "Replicated ranges differ for same blob location: " + replicatedRanges + " vs " + other.replicatedRanges;
        return replicatedRanges;
    }

    private static boolean assertNoOverlappingReplicatedRanges(TreeMap<Long, ReplicatedByteRange> ranges) {
        ReplicatedByteRange previous = null;
        for (var range : ranges.entrySet()) {
            assert previous == null || previous.copy + previous.length <= range.getValue().copy : previous + " vs " + range;
            previous = range.getValue();
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        BlobFileRanges that = (BlobFileRanges) o;
        return Objects.equals(blobLocation, that.blobLocation)
            && Objects.equals(replicatedRanges, that.replicatedRanges)
            && Objects.equals(timestampRange, that.timestampRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blobLocation, replicatedRanges, timestampRange);
    }

    @Override
    public String toString() {
        return "BlobFileRanges{"
            + "blobLocation="
            + blobLocation
            + ", replicatedRanges="
            + replicatedRanges
            + ", timestampRange="
            + timestampRange
            + '}';
    }

    // for tests only
    public @Nullable Long locationOfFirstReplicatedContents() {
        return replicatedRanges.isEmpty() ? null : replicatedRanges.firstEntry().getValue().copy;
    }

    public void forEachReplicatedRange(BiConsumer<Long, Short> consumer) {
        replicatedRanges.forEach((position, replicated) -> consumer.accept(replicated.copy(), replicated.length()));
    }
}
