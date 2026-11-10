/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.stateless.TestUtils.getCommitWithInternalFilesReplicatedRanges;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CommitPointerTests extends ESTestCase {

    private static CommitPointer randomPointer(ShardId shardId, long term) {
        return randomPointer(shardId, term, randomLongBetween(1, 1000));
    }

    private static CommitPointer randomPointer(ShardId shardId, long term, long generation) {
        final var termAndGen = new PrimaryTermAndGeneration(term, generation);
        final var cc = getCommitWithInternalFilesReplicatedRanges(
            shardId,
            new BlobFile(BatchedCompoundCommit.PREFIX + generation, termAndGen),
            "_na_",
            0,
            ByteSizeValue.ofKb(4).getBytes()
        );
        final var bcc = new BatchedCompoundCommit(termAndGen, List.of(cc));
        return CommitPointer.create(term, "node-" + randomAlphaOfLength(5), bcc, CommitPointer.computeReplicatedFileRanges(bcc));
    }

    private static ShardId randomShardId() {
        return new ShardId(new Index(randomAlphaOfLength(5), randomUUID()), randomIntBetween(0, 5));
    }

    public void testRoundTrip() throws IOException {
        final ShardId shardId = randomShardId();
        final long term = randomLongBetween(1, 10);
        final CommitPointer pointer = randomPointer(shardId, term);
        assertThat("replicated header/footer ranges are carried", pointer.replicatedFileRanges().size(), greaterThan(0));
        final BytesReference bytes = pointer.toBytes();
        final CommitPointer read = CommitPointer.fromBytes(bytes);
        assertThat(read.writerPrimaryTerm(), equalTo(term));
        assertThat(read.writerNodeId(), equalTo(pointer.writerNodeId()));
        assertThat(read.batchedCompoundCommitTermAndGeneration(), equalTo(pointer.batchedCompoundCommitTermAndGeneration()));
        assertThat(read.compoundCommit(), equalTo(pointer.compoundCommit()));
        assertThat(read.replicatedFileRanges().keySet(), equalTo(pointer.replicatedFileRanges().keySet()));
        for (var entry : pointer.replicatedFileRanges().entrySet()) {
            var other = read.replicatedFileRanges().get(entry.getKey());
            assertThat(other.blobLocation(), equalTo(entry.getValue().blobLocation()));
            long pos = entry.getValue().fileOffset();
            assertThat(other.getPosition(pos, 16), equalTo(entry.getValue().getPosition(pos, 16)));
        }
        // map iteration order makes the serialization non-canonical; a second round trip must read the same pointer
        final CommitPointer reread = CommitPointer.fromBytes(read.toBytes());
        assertThat(reread.compoundCommit(), equalTo(pointer.compoundCommit()));
        assertThat(reread.replicatedFileRanges().keySet(), equalTo(pointer.replicatedFileRanges().keySet()));
        assertThat(read.validate(shardId, term), nullValue());
    }

    /**
     * Phase 4: a pointer with open-time bytes round trips (format version 2), a pointer without them keeps format version 1, and open
     * ranges of a blob the commit does not reference are rejected.
     */
    public void testRoundTripWithOpenRanges() throws IOException {
        final ShardId shardId = randomShardId();
        final long term = randomLongBetween(1, 10);
        final CommitPointer plain = randomPointer(shardId, term);
        final BlobFile blobFile = plain.compoundCommit().commitFiles().values().iterator().next().blobFile();
        final List<CommitPointerOpenBytes.OpenRange> ranges = new java.util.ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            ranges.add(new CommitPointerOpenBytes.OpenRange(blobFile, 4096L * i * 2, new BytesArray(randomByteArrayOfLength(4096))));
        }
        final CommitPointer pointer = plain.withOpenRanges(ranges);
        assertThat(pointer.validate(shardId, term), nullValue());
        assertThat(pointer.openRangesSizeInBytes(), equalTo(4096L * ranges.size()));

        final CommitPointer read = CommitPointer.fromBytes(pointer.toBytes());
        assertThat(read.compoundCommit(), equalTo(pointer.compoundCommit()));
        assertThat(read.openRanges().size(), equalTo(ranges.size()));
        for (int i = 0; i < ranges.size(); i++) {
            assertThat(read.openRanges().get(i).blobFile(), equalTo(blobFile));
            assertThat(read.openRanges().get(i).position(), equalTo(ranges.get(i).position()));
            assertThat(read.openRanges().get(i).bytes(), equalTo(ranges.get(i).bytes()));
        }
        assertThat(read.validate(shardId, term), nullValue());
        // without open ranges the serialization is the one of format version 1 (only the trailing open ranges differ)
        assertThat(CommitPointer.fromBytes(plain.toBytes()).openRanges().isEmpty(), equalTo(true));
        assertThat(plain.toBytes().length(), lessThan(pointer.toBytes().length()));

        final var foreign = new BlobFile(BatchedCompoundCommit.PREFIX + 99999, new PrimaryTermAndGeneration(term, 99999));
        final CommitPointer invalid = plain.withOpenRanges(
            List.of(new CommitPointerOpenBytes.OpenRange(foreign, 0, new BytesArray(new byte[16])))
        );
        assertThat(invalid.validate(shardId, term), containsString("not referenced by the commit"));
    }

    public void testCorruptionIsDetected() throws IOException {
        final CommitPointer pointer = randomPointer(randomShardId(), randomLongBetween(1, 10));
        final byte[] bytes = BytesReference.toBytes(pointer.toBytes());
        final byte[] corrupted = bytes.clone();
        corrupted[randomIntBetween(0, corrupted.length - 1)] ^= (byte) randomIntBetween(1, 255);
        expectThrows(IOException.class, () -> CommitPointer.fromBytes(new BytesArray(corrupted)));
        final int truncatedLength = randomIntBetween(0, bytes.length - 1);
        expectThrows(Exception.class, () -> CommitPointer.fromBytes(new BytesArray(bytes, 0, truncatedLength)));
    }

    public void testValidateRejectsMismatches() {
        final ShardId shardId = randomShardId();
        final long term = randomLongBetween(2, 10);
        final CommitPointer pointer = randomPointer(shardId, term);
        assertThat(pointer.validate(shardId, term), nullValue());
        // the cluster state knows an older or newer primary term than the writer's
        assertThat(pointer.validate(shardId, term - 1), notNullValue());
        assertThat(pointer.validate(shardId, term + 1), containsString("primary term"));
        // another shard / another index with the same name
        assertThat(pointer.validate(new ShardId(shardId.getIndex(), shardId.id() + 1), term), notNullValue());
        assertThat(pointer.validate(new ShardId(new Index(shardId.getIndexName(), randomUUID()), shardId.id()), term), notNullValue());
        // a BCC that does not contain the commit
        final CommitPointer wrongBcc = new CommitPointer(
            term,
            pointer.writerNodeId(),
            new PrimaryTermAndGeneration(term, pointer.batchedCompoundCommitTermAndGeneration().generation() + 1),
            pointer.compoundCommit(),
            pointer.replicatedFileRanges()
        );
        assertThat(wrongBcc.validate(shardId, term), notNullValue());
    }

    /**
     * Phase 3 gap 1: the current primary (term {@code k}) recovered hollow from a commit of an older term and points at it; an isolated
     * primary of an older term uploaded a newer commit afterwards and published it in the pointer of its own term.
     */
    public void testCheckPredecessorPointers() throws IOException {
        final ShardId shardId = randomShardId();
        final BlobContainer container = new FsBlobStore(4096, createTempDir(), false).blobContainer(BlobPath.EMPTY);
        final long commitTerm = randomLongBetween(1, 5);
        final long knownTerm = commitTerm + randomLongBetween(1, CommitPointer.MAX_PREDECESSOR_POINTERS);
        final CommitPointer older = randomPointer(shardId, commitTerm);
        final StatelessCompoundCommit commit = older.compoundCommit();
        // the current primary's pointer names the older-term commit
        recreate(older, knownTerm).write(container);

        // commit of the current term: nothing to check
        assertThat(CommitPointer.checkPredecessorPointers(container, shardId, commitTerm, commit), nullValue());
        // no pointer of an older term: no evidence of a newer commit
        assertThat(CommitPointer.checkPredecessorPointers(container, shardId, knownTerm, commit), nullValue());
        // the older term's primary pointed at the same commit
        older.write(container);
        assertThat(CommitPointer.checkPredecessorPointers(container, shardId, knownTerm, commit), nullValue());

        // the older term's primary (stale, isolated) uploaded a newer commit of its term
        final CommitPointer newer = randomPointer(shardId, commitTerm, commit.generation() + randomLongBetween(1, 10));
        newer.write(container);
        assertThat(CommitPointer.checkPredecessorPointers(container, shardId, knownTerm, commit), containsString("newer than"));
        // and so did a primary of a term between the commit's and the current one
        if (knownTerm - commitTerm > 1) {
            older.write(container);
            final long middleTerm = randomLongBetween(commitTerm + 1, knownTerm - 1);
            randomPointer(shardId, middleTerm, randomLongBetween(1, 1000)).write(container);
            assertThat(CommitPointer.checkPredecessorPointers(container, shardId, knownTerm, commit), containsString("newer than"));
        }

        // an older term's pointer that cannot be trusted: do not use the commit
        final BlobContainer other = new FsBlobStore(4096, createTempDir(), false).blobContainer(BlobPath.EMPTY);
        other.writeBlobAtomic(OperationPurpose.INDICES, CommitPointer.blobName(commitTerm), new BytesArray(new byte[] { 1, 2, 3 }), false);
        assertThat(CommitPointer.checkPredecessorPointers(other, shardId, knownTerm, commit), containsString("unreadable"));
        // pointer stored under the older term's name but written in another term
        other.writeBlobAtomic(
            OperationPurpose.INDICES,
            CommitPointer.blobName(commitTerm),
            recreate(older, commitTerm + 100).toBytes(),
            false
        );
        assertThat(CommitPointer.checkPredecessorPointers(other, shardId, knownTerm, commit), containsString("invalid"));

        // too many older terms to check
        assertThat(
            CommitPointer.checkPredecessorPointers(container, shardId, commitTerm + CommitPointer.MAX_PREDECESSOR_POINTERS + 1, commit),
            containsString("terms older")
        );
    }

    private static CommitPointer recreate(CommitPointer pointer, long writerTerm) {
        return new CommitPointer(
            writerTerm,
            pointer.writerNodeId(),
            pointer.batchedCompoundCommitTermAndGeneration(),
            pointer.compoundCommit(),
            pointer.replicatedFileRanges()
        );
    }
}
