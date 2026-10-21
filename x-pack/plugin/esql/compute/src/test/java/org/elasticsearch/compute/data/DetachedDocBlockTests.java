/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
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

    public void testCopyFromInternsOnlyReferencedOrigins() {
        try (
            DocBlock source = build(List.of(a, b, c), true, new int[][] { { 0, 1, 2 }, { 1, 3, 4 }, { 2, 5, 6 }, { 0, 7, 8 } });
            DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 2)
        ) {
            builder.copyFrom(source, 2, 4);
            try (DocBlock copy = builder.build(DocVector.config().mayContainDuplicates()); Block expected = source.slice(2, 4)) {
                assertThat(copy.asVector().origins().size(), equalTo(2));
                assertThat(copy, equalTo(expected));
            }
        }
    }

    /**
     * Rows copied from vectors with different dictionaries share one dictionary in the copy.
     */
    public void testCopyFromMergesDictionaries() {
        try (
            DocBlock first = build(List.of(a, b), false, new int[][] { { 0, 1, 1 }, { 1, 2, 2 } });
            DocBlock second = build(List.of(c, b), false, new int[][] { { 0, 3, 3 }, { 1, 4, 4 } });
            DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 4)
        ) {
            builder.copyFrom(first, 0, 2);
            builder.copyFrom(second, 0, 2);
            try (DocBlock copy = builder.build()) {
                assertThat(copy.asVector().origins().size(), equalTo(3));
                assertThat(BlockUtils.toJavaObject(copy, 0), equalTo(new BlockUtils.DocRef(a, 1, 1)));
                assertThat(BlockUtils.toJavaObject(copy, 1), equalTo(new BlockUtils.DocRef(b, 2, 2)));
                assertThat(BlockUtils.toJavaObject(copy, 2), equalTo(new BlockUtils.DocRef(c, 3, 3)));
                assertThat(BlockUtils.toJavaObject(copy, 3), equalTo(new BlockUtils.DocRef(b, 4, 4)));
            }
        }
    }

    public void testCopyFromCantMixRowsOfThisNodeWithRowsThatLeftTheirs() {
        try (
            DocBlock detached = build(List.of(a), false, new int[][] { { 0, 1, 1 } });
            DocBlock local = local(new int[][] { { 0, 2, 2 } })
        ) {
            try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 2)) {
                builder.copyFrom(local, 0, 1);
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.copyFrom(detached, 0, 1));
                assertThat(e.getMessage(), equalTo("can't mix documents of this node with documents that left their node"));
            }
            try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 2)) {
                builder.copyFrom(detached, 0, 1);
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.copyFrom(local, 0, 1));
                assertThat(e.getMessage(), equalTo("can't mix documents of this node with documents that left their node"));
            }
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
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(DocBlock.ESQL_DOC_ORIGINS);
        try (DocBlock block = (DocBlock) random.block(); DocBlock read = serializeDeserializeBlockWithVersion(block, version)) {
            assertThat(read, equalTo(block));
            assertThat(read.asVector().mayContainDuplicates(), equalTo(block.asVector().mayContainDuplicates()));
            assertThat(read.asVector().origins().size(), lessThanOrEqualTo(block.asVector().origins().size()));
            assertThat(blockFactory.breaker().getUsed(), equalTo(block.ramBytesUsed() + read.ramBytesUsed()));
        }
    }

    public void testSerializationDropsOriginsNoRowUses() throws IOException {
        RandomBlock random = RandomBlock.randomDetachedDocBlock(blockFactory, 1000, 100);
        try (
            DocBlock block = (DocBlock) random.block();
            DocBlock filtered = (DocBlock) block.filter(true, 0, 500, 999);
            DocBlock read = serializeDeserializeBlockWithVersion(filtered, DocBlock.ESQL_DOC_ORIGINS)
        ) {
            assertThat(filtered.asVector().origins().size(), equalTo(100));
            assertThat(read.asVector().origins().size(), lessThanOrEqualTo(3));
            assertThat(read, equalTo(filtered));
        }
    }

    /**
     * Rows of one origin need no ordinal on the wire, however their shard vector was built. They read back as constant
     * ordinals.
     */
    public void testOneOriginWritesNoOrdinals() throws IOException {
        int positions = between(100, 500);
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendShard(1).appendSegment(between(0, 20)).appendDoc(p);
            }
            DocVector.Config config = DocVector.config().origins(DocOrigins.of(List.of(a, b)));
            try (
                DocBlock block = builder.build(config);
                BytesStreamOutput out = new BytesStreamOutput();
                BytesStreamOutput parts = new BytesStreamOutput()
            ) {
                assertFalse(block.asVector().shards().isConstant());
                Block.writeTypedBlock(block, out);
                b.writeTo(parts);
                block.asVector().segments().writeTo(parts);
                block.asVector().docs().writeTo(parts);
                // the element type, the flags and the two counts
                assertThat(out.bytes().length() - parts.bytes().length(), lessThan(8));

                try (DocBlock read = serializeDeserializeBlockWithVersion(block, DocBlock.ESQL_DOC_ORIGINS)) {
                    assertTrue(read.asVector().shards().isConstant());
                    assertThat(read.asVector().origins().size(), equalTo(1));
                    assertThat(read, equalTo(block));
                }
            }
        }
    }

    public void testOlderNodesCantReadIt() {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(DocBlock.ESQL_DOC_ORIGINS);
        try (DocBlock block = build(List.of(a), false, new int[][] { { 0, 0, 0 } }); BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(old);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> Block.writeTypedBlock(block, out));
            assertThat(e.getMessage(), containsString("can't send documents to a node on"));
        }
    }

    public void testRowsOfThisNodeCantBeWritten() {
        try (DocBlock block = local(new int[][] { { 0, 0, 0 } }); BytesStreamOutput out = new BytesStreamOutput()) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> Block.writeTypedBlock(block, out));
            assertThat(e.getMessage(), equalTo("can't write documents that are still on the node that read them, detach them first"));
        }
    }

    /**
     * An empty block names no shard, so it can cross the wire. It arrives as a block whose rows left their node.
     */
    public void testEmptyBlockOfThisNode() throws IOException {
        try (DocBlock block = local(new int[0][]); DocBlock read = serializeDeserializeBlockWithVersion(block, DocBlock.ESQL_DOC_ORIGINS)) {
            assertThat(read.getPositionCount(), equalTo(0));
            assertThat(read.asVector().origins().size(), equalTo(0));
        }
    }

    public void testRejectsUnknownFlags() {
        expectCorrupt("unknown doc block flags", out -> {
            out.writeByte((byte) 2);
            out.writeVInt(0);
            out.writeVInt(0);
        });
    }

    public void testRejectsMoreOriginsThanRows() {
        expectCorrupt("invalid doc block with [2] positions and [3] origins", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(3);
        });
    }

    public void testRejectsRowsWithoutOrigins() {
        expectCorrupt("invalid doc block with [2] positions and [0] origins", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(0);
        });
    }

    public void testRejectsOriginsOfAnEmptyBlock() {
        expectCorrupt("invalid doc block with [0] positions and [1] origins", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(0);
            out.writeVInt(1);
        });
    }

    public void testRejectsDuplicateOrigins() {
        expectCorrupt("duplicate origin", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(2);
            a.writeTo(out);
            a.writeTo(out);
        });
    }

    public void testRejectsOrdinalsOutOfRange() {
        expectCorrupt("origin ordinal [5] out of [0, 2)", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(2);
            a.writeTo(out);
            b.writeTo(out);
            out.writeVInt(0);
            out.writeVInt(5);
        });
    }

    public void testRejectsNegativeSegments() {
        expectCorrupt("negative value [-3] in segments", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(1);
            a.writeTo(out);
            writeIntVector(out, 0, -3);
            writeIntVector(out, 7, 8);
        });
    }

    public void testRejectsNegativeDocs() {
        expectCorrupt("negative value [-1] in docs", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(1);
            a.writeTo(out);
            writeIntVector(out, 0, 0);
            writeIntVector(out, 7, -1);
        });
    }

    public void testRejectsVectorsOfTheWrongLength() {
        expectCorrupt("expected [2] segments but got [3]", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(1);
            a.writeTo(out);
            writeIntVector(out, 0, 0, 0);
            writeIntVector(out, 1, 2);
        });
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

    private void writeIntVector(StreamOutput out, int... values) throws IOException {
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(values.length)) {
            for (int value : values) {
                builder.appendInt(value);
            }
            try (IntVector vector = builder.build()) {
                vector.writeTo(out);
            }
        }
    }

    private void expectCorrupt(String message, CheckedConsumer<StreamOutput, IOException> writer) {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                writer.accept(out);
                try (BlockStreamInput in = blockStreamInput(out); DocBlock read = DocBlock.readFrom(in)) {
                    fail("read a corrupt block " + read);
                }
            }
        });
        assertThat(e.getMessage(), containsString(message));
        assertThat("a rejected block leaves nothing on the breaker", blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
