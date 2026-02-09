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

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a collection of compound commits stored together within the same blob.
 */
public record BatchedCompoundCommit(PrimaryTermAndGeneration primaryTermAndGeneration, List<StatelessCompoundCommit> compoundCommits)
    implements
        AbstractBatchedCompoundCommit {

    public BatchedCompoundCommit {
        if (primaryTermAndGeneration == null) {
            throw new IllegalArgumentException("Batched compound commits must have a non-null primary term and generation");
        }

        if (compoundCommits == null) {
            throw new IllegalArgumentException("Batched compound commits must have a non-null list of compound commits");
        }

        if (compoundCommits.isEmpty()) {
            throw new IllegalArgumentException("Batched compound commits must have a non-empty list of compound commits");
        }

        assert compoundCommits.stream().map(StatelessCompoundCommit::primaryTerm).distinct().count() == 1
            : "all compound commits must have the same primary term";

        assert IntStream.range(0, compoundCommits.size() - 1)
            .allMatch(
                i -> compoundCommits.get(i).primaryTermAndGeneration().compareTo(compoundCommits.get(i + 1).primaryTermAndGeneration()) < 0
            ) : "the list of compound commits must be sorted by their primary terms and generations";

        assert compoundCommits.stream().map(StatelessCompoundCommit::shardId).distinct().count() == 1
            : "all compound commits must be for the same shard";
    }

    public ShardId shardId() {
        return compoundCommits.getFirst().shardId();
    }

    public int size() {
        return compoundCommits.size();
    }

    @Override
    public StatelessCompoundCommit lastCompoundCommit() {
        return compoundCommits.getLast();
    }

    public Set<String> getAllInternalFiles() {
        return compoundCommits.stream().flatMap(commit -> commit.internalFiles().stream()).collect(Collectors.toSet());
    }

    /**
     * Reads a maximum of {@code maxBlobLength} bytes of a {@link BatchedCompoundCommit} from the blob store. For that it materializes the
     * headers for all the {@link StatelessCompoundCommit} contained in the batched compound commit that are located before the maximum blob
     * length.
     *
     * @param blobName          the blob name where the batched compound commit is stored
     * @param maxBlobLength     the maximum number of bytes to read for the blob (not expected to be in the middle of a header or internal
     *                          replicated range bytes)
     * @param blobReader        a blob reader
     * @param exactBlobLength   a flag indicating that the max. blob length is equal to the real blob length in the object store (flag is
     *                          {@code true}) or not (flag is {@code false}) in which case we are OK to not read the blob fully. This flag
     *                          is used in assertions only.
     * @return                  the {@link BatchedCompoundCommit} containing all the commits before {@code maxBlobLength}
     * @throws IOException      if an I/O exception is thrown while reading the blob
     */
    public static BatchedCompoundCommit readFromStore(String blobName, long maxBlobLength, BlobReader blobReader, boolean exactBlobLength)
        throws IOException {
        PrimaryTermAndGeneration primaryTermAndGeneration = null;
        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>();
        var bccCommitsIterator = readFromStoreIncrementally(blobName, maxBlobLength, blobReader, exactBlobLength);
        while (bccCommitsIterator.hasNext()) {
            var compoundCommit = bccCommitsIterator.next();
            // BatchedCompoundCommit uses the first StatelessCompoundCommit primary term and generation
            if (primaryTermAndGeneration == null) {
                primaryTermAndGeneration = compoundCommit.primaryTermAndGeneration();
            }
            compoundCommits.add(compoundCommit);
        }
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    /**
     * Creates an iterator that incrementally reads {@link StatelessCompoundCommit} headers from a batched compound commit blob
     * in the blob store, up to the specified maximum blob length. This method provides lazy loading of compound commits,
     * allowing for memory-efficient processing of large batched compound commits without materializing all commits at once.
     * <p>
     * The iterator will read and parse compound commit headers sequentially from the blob, stopping when either the maximum
     * blob length is reached or all compound commits in the blob have been processed. Each iteration returns a
     * {@link StatelessCompoundCommit} containing both the BCC primary term and generation and the compound commit header.
     * </p>
     *
     * <p>
     * This method is particularly useful when processing large batched compound commits where memory usage needs to be
     * controlled, or when only a subset of the compound commits in a blob need to be processed.
     * </p>
     *
     * @param blobName          the blob name where the batched compound commit is stored
     * @param maxBlobLength     the maximum number of bytes to read for the blob (not expected to be in the middle of a header or internal
     *                          replicated range bytes)
     * @param blobReader        a blob reader
     * @param exactBlobLength   a flag indicating that the max. blob length is equal to the real blob length in the object store (flag is
     *                          {@code true}) or not (flag is {@code false}) in which case we are OK to not read the blob fully. This flag
     *                          is used in assertions only.
     * @return                  an iterator over {@link StatelessCompoundCommit} objects that lazily reads compound commit headers
     *                          from the blob store up to the specified maximum length
     */
    public static Iterator<StatelessCompoundCommit> readFromStoreIncrementally(
        String blobName,
        long maxBlobLength,
        BlobReader blobReader,
        boolean exactBlobLength
    ) {
        return new BCCStatelessCompoundCommitsIterator(blobName, maxBlobLength, blobReader, exactBlobLength);
    }

    static class BCCStatelessCompoundCommitsIterator implements Iterator<StatelessCompoundCommit> {
        private final String blobName;
        private final long maxBlobLength;
        private final BlobReader blobReader;
        private final boolean exactBlobLength;
        private long offset;

        BCCStatelessCompoundCommitsIterator(String blobName, long maxBlobLength, BlobReader blobReader, boolean exactBlobLength) {
            this.blobName = blobName;
            this.maxBlobLength = maxBlobLength;
            this.blobReader = blobReader;
            this.exactBlobLength = exactBlobLength;
        }

        @Override
        public boolean hasNext() {
            assert offset < maxBlobLength || offset == BlobCacheUtils.toPageAlignedSize(maxBlobLength) || exactBlobLength == false
                : "offset "
                    + offset
                    + " != page-aligned blobLength "
                    + BlobCacheUtils.toPageAlignedSize(maxBlobLength)
                    + " with exact blob length flag [true]";
            return offset < maxBlobLength;
        }

        @Override
        public StatelessCompoundCommit next() {
            assert offset == BlobCacheUtils.toPageAlignedSize(offset) : "should only read page-aligned compound commits but got: " + offset;
            try (StreamInput streamInput = blobReader.readBlobAtOffset(blobName, offset, maxBlobLength - offset)) {
                var compoundCommit = StatelessCompoundCommit.readFromStoreAtOffset(
                    streamInput,
                    offset,
                    ignored -> StatelessCompoundCommit.parseGenerationFromBlobName(blobName)
                );

                assert assertPaddingComposedOfZeros(blobName, maxBlobLength, blobReader, offset, compoundCommit);
                offset += BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
                return compoundCommit;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static boolean assertPaddingComposedOfZeros(
        String blobName,
        long blobLength,
        BlobReader blobReader,
        long offset,
        StatelessCompoundCommit compoundCommit
    ) throws IOException {
        long compoundCommitSize = compoundCommit.sizeInBytes();
        long compoundCommitSizePageAligned = BlobCacheUtils.toPageAlignedSize(compoundCommitSize);
        int padding = Math.toIntExact(compoundCommitSizePageAligned - compoundCommitSize);
        assert padding >= 0 : "padding " + padding + " is negative";
        long paddingOffset = offset + compoundCommitSize;
        if (padding > 0 && paddingOffset < blobLength) {
            try (StreamInput paddingStreamInput = blobReader.readBlobAtOffset(blobName, paddingOffset, padding)) {
                byte[] paddingBytes = paddingStreamInput.readNBytes(padding);
                byte[] zeroBytes = new byte[padding];
                Arrays.fill(zeroBytes, (byte) 0);
                assert Arrays.equals(paddingBytes, zeroBytes);
            }
        }
        return true;
    }

    /**
     * An object that allows reading a blob at a given offset efficiently
     * (i.e. issuing a new call to the blob store API instead of reading all the bytes until the requested offset)
     */
    @FunctionalInterface
    public interface BlobReader {
        StreamInput readBlobAtOffset(String blobName, long offset, long length) throws IOException;
    }

    public static Set<PrimaryTermAndGeneration> computeReferencedBCCGenerations(StatelessCompoundCommit commit) {
        Set<PrimaryTermAndGeneration> primaryTermAndGenerations = new HashSet<>();
        for (BlobLocation blobLocation : commit.commitFiles().values()) {
            primaryTermAndGenerations.add(blobLocation.getBatchedCompoundCommitTermAndGeneration());
        }
        return Collections.unmodifiableSet(primaryTermAndGenerations);
    }

    public static String blobNameFromGeneration(long generation) {
        return StatelessCompoundCommit.blobNameFromGeneration(generation);
    }
}
