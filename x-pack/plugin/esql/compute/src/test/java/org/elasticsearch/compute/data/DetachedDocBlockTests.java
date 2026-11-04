/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link DocBlock}s whose rows left the node that read them, and their wire format. The base class checks that every
 * test leaves the breaker at zero.
 */
public class DetachedDocBlockTests extends SerializationTestCase {
    private final DocOrigin a = RandomBlock.randomDocOrigin();
    private final DocOrigin b = randomValueOtherThan(a, RandomBlock::randomDocOrigin);
    private final DocOrigin c = randomValueOtherThanMany(o -> o.equals(a) || o.equals(b), RandomBlock::randomDocOrigin);

    public void testValues() {
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, between(1, 200), between(1, 5));
        try (Block block = random.block()) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                assertThat(BlockUtils.toJavaObject(block, p), equalTo(random.values().get(p).get(0)));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(block.ramBytesUsed()));
        }
    }

    public void testEqualityIgnoresTheDictionaryLayout() {
        try (
            DocBlock ab = build(List.of(a, b), true, new int[][] { { 0, 1, 2 }, { 1, 3, 4 } });
            DocBlock baWithUnusedC = build(List.of(b, c, a), true, new int[][] { { 2, 1, 2 }, { 0, 3, 4 } });
            DocBlock other = build(List.of(a, b), true, new int[][] { { 0, 1, 2 }, { 0, 3, 4 } })
        ) {
            assertThat(ab, equalTo(baWithUnusedC));
            assertThat(ab.hashCode(), equalTo(baWithUnusedC.hashCode()));
            assertThat(ab, not(equalTo(other)));
        }
    }

    public void testRowsOfThisNodeNeverEqualRowsThatLeftTheirs() {
        try (
            DocBlock detached = build(List.of(a), false, new int[][] { { 0, 1, 2 } });
            DocBlock local = local(new int[][] { { 0, 1, 2 } })
        ) {
            assertThat(detached, not(equalTo(local)));
            assertThat(local, not(equalTo(detached)));
        }
    }

    public void testCopyFromRejectsDetachedBlocks() {
        try (
            DocBlock detached = build(List.of(a), false, new int[][] { { 0, 1, 2 } });
            DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 1)
        ) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.copyFrom(detached, 0, 1));
            assertThat(e.getMessage(), equalTo("[DocBlock.Builder#copyFrom] can't read a detached DocVector"));
        }
    }

    public void testFilterAndSliceShareTheDictionary() {
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, between(2, 100), between(1, 5));
        try (DocBlock block = (DocBlock) random.block()) {
            int last = block.getPositionCount() - 1;
            try (
                DocBlock filtered = (DocBlock) block.filter(false, 0, last);
                DocBlock sliced = (DocBlock) block.slice(1, block.getPositionCount())
            ) {
                assertThat(filtered.asVector().origins(), sameInstance(block.asVector().origins()));
                assertThat(sliced.asVector().origins(), sameInstance(block.asVector().origins()));
                assertThat(BlockUtils.toJavaObject(filtered, 1), equalTo(random.values().get(last).get(0)));
                assertThat(BlockUtils.toJavaObject(sliced, 0), equalTo(random.values().get(1).get(0)));
            }
        }
    }

    public void testDeepCopy() {
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, between(1, 100), between(1, 5));
        try (DocBlock block = (DocBlock) random.block(); DocBlock copy = (DocBlock) block.deepCopy(blockFactory)) {
            assertThat(copy, equalTo(block));
            assertThat(copy.asVector().origins(), sameInstance(block.asVector().origins()));
        }
    }

    public void testSerialization() throws IOException {
        int positions = between(0, 1000);
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, positions, between(1, 10));
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(DocVector.ESQL_DOC_ORIGINS);
        try (DocBlock block = (DocBlock) random.block(); DocBlock read = serializeDeserializeBlockWithVersion(block, version)) {
            assertThat(read, equalTo(block));
            assertThat(read.asVector().mayContainDuplicates(), equalTo(block.asVector().mayContainDuplicates()));
            assertThat(read.asVector().origins().size(), lessThanOrEqualTo(block.asVector().origins().size()));
            assertThat(blockFactory.breaker().getUsed(), equalTo(block.ramBytesUsed() + read.ramBytesUsed()));
        }
    }

    /**
     * A filtered or sliced vector shares the dictionary of its source, so it may hold origins no row references. They are
     * written as they are.
     */
    public void testSerializationKeepsOriginsNoRowReferences() throws IOException {
        try (
            DocBlock block = build(List.of(a, b, c), false, new int[][] { { 0, 1, 2 }, { 1, 3, 4 }, { 2, 5, 6 } });
            DocBlock filtered = (DocBlock) block.filter(false, 1);
            DocBlock read = serializeDeserializeBlockWithVersion(filtered, DocVector.ESQL_DOC_ORIGINS)
        ) {
            DocOrigins origins = read.asVector().origins();
            assertThat(List.of(origins.get(0), origins.get(1), origins.get(2)), equalTo(List.of(a, b, c)));
            assertThat(read.asVector().shards().getInt(0), equalTo(1));
            assertThat(read, equalTo(filtered));
        }
    }

    /**
     * The input ends inside the shard, segment or doc vectors, after the reader allocated some of them.
     */
    public void testTruncatedInputLeavesNothingOnTheBreaker() throws IOException {
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, between(10, 100), between(2, 5));
        try (
            DocBlock block = (DocBlock) random.block();
            BytesStreamOutput out = new BytesStreamOutput();
            BytesStreamOutput header = new BytesStreamOutput();
            BytesStreamOutput truncated = new BytesStreamOutput()
        ) {
            block.writeTo(out);
            header.writeBoolean(block.asVector().mayContainDuplicates());
            block.asVector().origins().writeTo(header);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            truncated.writeBytes(bytes, 0, between(header.size() + 1, bytes.length - 1));
            long usedBeforeRead = blockFactory.breaker().getUsed();
            try (BlockStreamInput in = blockStreamInput(truncated)) {
                expectThrows(EOFException.class, () -> DocBlock.readFrom(in));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(usedBeforeRead));
        }
    }

    public void testOlderNodesCantReadIt() {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(DocVector.ESQL_DOC_ORIGINS);
        try (DocBlock block = build(List.of(a), false, new int[][] { { 0, 0, 0 } }); BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(old);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> Block.writeTypedBlock(block, out));
            assertThat(e.getMessage(), containsString("can't send documents to a node on"));
        }
    }

    public void testRowsOfThisNodeCantBeWritten() {
        try (DocBlock block = local(new int[][] { { 0, 0, 0 } }); BytesStreamOutput out = new BytesStreamOutput()) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> Block.writeTypedBlock(block, out));
            assertThat(e.getMessage(), equalTo("DocVector has no origins"));
        }
    }

    /**
     * An empty block names no shard, so it can cross the wire. It arrives as a block whose rows left their node.
     */
    public void testEmptyBlockOfThisNode() throws IOException {
        try (
            DocBlock block = local(new int[0][]);
            DocBlock read = serializeDeserializeBlockWithVersion(block, DocVector.ESQL_DOC_ORIGINS)
        ) {
            assertThat(read.getPositionCount(), equalTo(0));
            assertThat(read.asVector().origins().size(), equalTo(0));
        }
    }

    private DocBlock build(List<DocOrigin> origins, boolean mayContainDuplicates, int[][] rows) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, rows.length)) {
            for (int[] row : rows) {
                builder.appendShard(row[0]).appendSegment(row[1]).appendDoc(row[2]);
            }
            return builder.build(DocVector.config().origins(DocOrigins.of(origins)).mayContainDuplicates(mayContainDuplicates));
        }
    }

    /**
     * A block of rows still on the node that read them.
     */
    private DocBlock local(int[][] rows) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, rows.length)) {
            for (int[] row : rows) {
                builder.appendShard(row[0]).appendSegment(row[1]).appendDoc(row[2]);
            }
            return builder.build();
        }
    }
}
