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
        return compoundCommits.get(0).shardId();
    }

    public int size() {
        return compoundCommits.size();
    }

    public StatelessCompoundCommit last() {
        return compoundCommits.get(compoundCommits.size() - 1);
    }

    @Override
    public StatelessCompoundCommit lastCompoundCommit() {
        return last();
    }

    public Set<String> getAllInternalFiles() {
        return compoundCommits.stream().flatMap(commit -> commit.internalFiles().stream()).collect(Collectors.toSet());
    }

    /**
     * Reads a {@link BatchedCompoundCommit} from the blob store, for that it materializes the headers
     * for all the {@link StatelessCompoundCommit} contained in the batched compound commit.
     *
     * @param blobName the blob name where the batched compound commit is store
     * @param blobLength the batched compound commit blob length in bytes
     * @param blobReader a blob reader
     */
    public static BatchedCompoundCommit readFromStore(String blobName, long blobLength, BlobReader blobReader) throws IOException {
        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>();
        long offset = 0;
        PrimaryTermAndGeneration primaryTermAndGeneration = null;
        while (offset < blobLength) {
            assert offset == BlobCacheUtils.toPageAlignedSize(offset) : "should only read page-aligned compound commits but got: " + offset;
            try (StreamInput streamInput = blobReader.readBlobAtOffset(blobName, offset, blobLength - offset)) {
                var compoundCommit = StatelessCompoundCommit.readFromStoreAtOffset(streamInput, offset, ignored -> blobName);
                // BatchedCompoundCommit uses the first StatelessCompoundCommit primary term and generation
                if (primaryTermAndGeneration == null) {
                    primaryTermAndGeneration = compoundCommit.primaryTermAndGeneration();
                }
                compoundCommits.add(compoundCommit);
                assert assertPaddingComposedOfZeros(blobName, blobLength, blobReader, offset, compoundCommit);
                offset += BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
            }
        }
        assert offset == BlobCacheUtils.toPageAlignedSize(blobLength)
            : "offset " + offset + " != page-aligned blobLength " + BlobCacheUtils.toPageAlignedSize(blobLength);
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
            var generation = StatelessCompoundCommit.parseGenerationFromBlobName(blobLocation.blobName());
            primaryTermAndGenerations.add(new PrimaryTermAndGeneration(blobLocation.primaryTerm(), generation));
        }
        return Collections.unmodifiableSet(primaryTermAndGenerations);
    }

    public static String blobNameFromGeneration(long generation) {
        return StatelessCompoundCommit.blobNameFromGeneration(generation);
    }
}
