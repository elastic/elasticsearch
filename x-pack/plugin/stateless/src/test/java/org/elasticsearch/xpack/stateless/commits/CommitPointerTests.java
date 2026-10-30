/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CommitPointerTests extends ESTestCase {

    private static CommitPointer randomPointer(ShardId shardId, long term) {
        final long generation = randomLongBetween(1, 1000);
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
}
