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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents a collection of compound commits stored together within the same blob.
 */
public record BatchedCompoundCommit(PrimaryTermAndGeneration primaryTermAndGeneration, List<StatelessCompoundCommit> compoundCommits) {

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

    public StatelessCompoundCommit getLast() {
        return compoundCommits.get(compoundCommits.size() - 1);
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
            try (StreamInput streamInput = blobReader.readBlobAtOffset(blobName, offset, blobLength - offset)) {
                var compoundCommit = StatelessCompoundCommit.readFromStoreAtOffset(streamInput, offset, ignored -> blobName);
                // BatchedCompoundCommit uses the first StatelessCompoundCommit primary term and generation
                if (primaryTermAndGeneration == null) {
                    primaryTermAndGeneration = compoundCommit.primaryTermAndGeneration();
                }
                offset += compoundCommit.sizeInBytes();
                compoundCommits.add(compoundCommit);
            }
        }
        assert offset == blobLength;
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    /**
     * An object that allows reading a blob at a given offset efficiently
     * (i.e. issuing a new call to the blob store API instead of reading all the bytes until the requested offset)
     */
    @FunctionalInterface
    public interface BlobReader {
        StreamInput readBlobAtOffset(String blobName, long offset, long length) throws IOException;
    }
}
