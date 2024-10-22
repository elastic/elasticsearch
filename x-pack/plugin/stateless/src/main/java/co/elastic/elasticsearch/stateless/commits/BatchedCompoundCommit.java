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

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>();
        long offset = 0;
        PrimaryTermAndGeneration primaryTermAndGeneration = null;
        while (offset < maxBlobLength) {
            assert offset == BlobCacheUtils.toPageAlignedSize(offset) : "should only read page-aligned compound commits but got: " + offset;
            try (StreamInput streamInput = blobReader.readBlobAtOffset(blobName, offset, maxBlobLength - offset)) {
                var compoundCommit = StatelessCompoundCommit.readFromStoreAtOffset(
                    streamInput,
                    offset,
                    ignored -> StatelessCompoundCommit.parseGenerationFromBlobName(blobName)
                );
                // BatchedCompoundCommit uses the first StatelessCompoundCommit primary term and generation
                if (primaryTermAndGeneration == null) {
                    primaryTermAndGeneration = compoundCommit.primaryTermAndGeneration();
                }
                compoundCommits.add(compoundCommit);
                assert assertPaddingComposedOfZeros(blobName, maxBlobLength, blobReader, offset, compoundCommit);
                offset += BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
            }
        }
        assert offset == BlobCacheUtils.toPageAlignedSize(maxBlobLength) || exactBlobLength == false
            : "offset "
                + offset
                + " != page-aligned blobLength "
                + BlobCacheUtils.toPageAlignedSize(maxBlobLength)
                + " with exact blob length flag [true]";
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
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
