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
import org.elasticsearch.common.unit.ByteSizeValue;
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
 * {@link DocRefBlock} and {@link DocRefVector}. The base class checks that every test leaves the breaker at zero.
 */
public class DocRefBlockTests extends SerializationTestCase {
    private final DocRefOrigin a = RandomBlock.randomDocRefOrigin();
    private final DocRefOrigin b = randomValueOtherThan(a, RandomBlock::randomDocRefOrigin);
    private final DocRefOrigin c = randomValueOtherThanMany(o -> o.equals(a) || o.equals(b), RandomBlock::randomDocRefOrigin);

    public void testOneOriginBuildsConstantOrdinals() {
        int positions = between(1, 100);
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, positions)) {
            int ordinal = builder.addOrigin(a);
            for (int p = 0; p < positions; p++) {
                builder.append(ordinal, between(0, 3), p);
            }
            try (DocRefBlock block = builder.build()) {
                assertTrue(block.asVector().singleOrigin());
                assertTrue(block.asVector().originOrdinals().isConstant());
                assertThat(block.asVector().origin(positions - 1), equalTo(a));
                assertThat(blockFactory.breaker().getUsed(), equalTo(block.ramBytesUsed()));
            }
        }
    }

    public void testValues() {
        RandomBlock random = RandomBlock.randomDocRefBlock(blockFactory, between(1, 200), between(1, 5));
        try (Block block = random.block()) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                assertThat(BlockUtils.toJavaObject(block, p), equalTo(random.values().get(p).get(0)));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(block.ramBytesUsed()));
        }
    }

    public void testEqualityIgnoresTheDictionaryLayout() {
        try (
            DocRefBlock ab = build(List.of(a, b), true, new int[][] { { 0, 1, 2 }, { 1, 3, 4 } });
            DocRefBlock baWithUnusedC = build(List.of(b, c, a), false, new int[][] { { 2, 1, 2 }, { 0, 3, 4 } });
            DocRefBlock other = build(List.of(a, b), true, new int[][] { { 0, 1, 2 }, { 0, 3, 4 } })
        ) {
            assertThat(ab, equalTo(baWithUnusedC));
            assertThat(ab.hashCode(), equalTo(baWithUnusedC.hashCode()));
            assertThat(ab, not(equalTo(other)));
        }
    }

    public void testCopyFromInternsOnlyReferencedOrigins() {
        try (
            DocRefBlock source = build(List.of(a, b, c), true, new int[][] { { 0, 1, 2 }, { 1, 3, 4 }, { 2, 5, 6 }, { 0, 7, 8 } });
            DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, 2)
        ) {
            builder.copyFrom(source, 2, 4);
            try (DocRefBlock copy = builder.build(); DocRefBlock expected = source.slice(2, 4)) {
                assertThat(copy.asVector().origins().size(), equalTo(2));
                assertThat(copy, equalTo(expected));
            }
        }
    }

    public void testFilterAndSliceShareTheDictionary() {
        RandomBlock random = RandomBlock.randomDocRefBlock(blockFactory, between(2, 100), between(1, 5));
        try (DocRefBlock block = (DocRefBlock) random.block()) {
            int[] positions = { 0, block.getPositionCount() - 1 };
            try (DocRefBlock filtered = block.filter(false, positions); DocRefBlock sliced = block.slice(1, block.getPositionCount())) {
                assertThat(filtered.asVector().origins(), sameInstance(block.asVector().origins()));
                assertThat(sliced.asVector().origins(), sameInstance(block.asVector().origins()));
                assertThat(BlockUtils.toJavaObject(filtered, 1), equalTo(random.values().get(block.getPositionCount() - 1).get(0)));
                assertThat(BlockUtils.toJavaObject(sliced, 0), equalTo(random.values().get(1).get(0)));
            }
            try (DocRefBlock whole = block.slice(0, block.getPositionCount())) {
                assertThat("slicing the whole block shares the vector", whole.asVector(), sameInstance(block.asVector()));
            }
        }
    }

    public void testDeepCopy() {
        RandomBlock random = RandomBlock.randomDocRefBlock(blockFactory, between(1, 100), between(1, 5));
        try (DocRefBlock block = (DocRefBlock) random.block(); DocRefBlock copy = block.deepCopy(blockFactory)) {
            assertThat(copy, equalTo(block));
            assertThat(copy.asVector().mayContainDuplicates(), equalTo(block.asVector().mayContainDuplicates()));
        }
    }

    public void testExpandIsIdentity() {
        try (DocRefBlock block = build(List.of(a), true, new int[][] { { 0, 0, 0 } }); DocRefBlock expanded = block.expand()) {
            assertThat(expanded, sameInstance(block));
        }
    }

    public void testNoNullsAndOneValuePerPosition() {
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, 1)) {
            expectThrows(UnsupportedOperationException.class, builder::appendNull);
            expectThrows(UnsupportedOperationException.class, builder::beginPositionEntry);
            expectThrows(IllegalArgumentException.class, () -> builder.append(0, 0, 0));
        }
        try (DocRefBlock block = build(List.of(a), true, new int[][] { { 0, 0, 0 } })) {
            try (IntVector before = blockFactory.newConstantIntVector(0, 1)) {
                expectThrows(UnsupportedOperationException.class, () -> block.insertNulls(before));
            }
            try (BooleanVector mask = blockFactory.newConstantBooleanVector(true, 1)) {
                expectThrows(UnsupportedOperationException.class, () -> block.keepMask(mask));
            }
            try (IntBlock positions = blockFactory.newConstantIntBlockWith(0, 1)) {
                expectThrows(UnsupportedOperationException.class, () -> block.lookup(positions, ByteSizeValue.ofKb(1)));
            }
        }
    }

    public void testSerialization() throws IOException {
        int positions = between(0, 1000);
        RandomBlock random = RandomBlock.randomDocRefBlock(blockFactory, positions, between(1, 10));
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(DocRefBlock.ESQL_DOC_REF);
        try (DocRefBlock block = (DocRefBlock) random.block(); DocRefBlock read = serializeDeserializeBlockWithVersion(block, version)) {
            assertThat(read, equalTo(block));
            assertThat(read.asVector().mayContainDuplicates(), equalTo(block.asVector().mayContainDuplicates()));
            assertThat(read.asVector().origins().size(), lessThanOrEqualTo(block.asVector().origins().size()));
        }
    }

    public void testSerializationDropsOriginsNoRowUses() throws IOException {
        RandomBlock random = RandomBlock.randomDocRefBlock(blockFactory, 1000, 100);
        try (
            DocRefBlock block = (DocRefBlock) random.block();
            DocRefBlock filtered = block.filter(true, 0, 500, 999);
            DocRefBlock read = serializeDeserializeBlockWithVersion(filtered, DocRefBlock.ESQL_DOC_REF)
        ) {
            assertThat(filtered.asVector().origins().size(), equalTo(100));
            assertThat(read.asVector().origins().size(), lessThanOrEqualTo(3));
            assertThat(read, equalTo(filtered));
        }
    }

    public void testOneOriginIsSmallOnTheWire() throws IOException {
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, 500)) {
            int ordinal = builder.addOrigin(a);
            for (int p = 0; p < 500; p++) {
                builder.append(ordinal, between(0, 20), between(0, 1_000_000));
            }
            try (DocRefBlock block = builder.build(); BytesStreamOutput out = new BytesStreamOutput()) {
                Block.writeTypedBlock(block, out);
                // a segment and a doc per row plus one origin, against 60 to 90 bytes per row for a string handle
                assertThat(out.bytes().length(), lessThan(5 * 1024));
            }
        }
    }

    public void testOlderNodesCantReadIt() {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(DocRefBlock.ESQL_DOC_REF);
        try (DocRefBlock block = build(List.of(a), true, new int[][] { { 0, 0, 0 } }); BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(old);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> Block.writeTypedBlock(block, out));
            assertThat(e.getMessage(), containsString("can't send document references"));
        }
    }

    public void testRejectsUnknownFlags() {
        expectCorrupt("unknown doc ref block flags", out -> {
            out.writeByte((byte) 2);
            out.writeVInt(0);
            out.writeVInt(0);
        });
    }

    public void testRejectsMoreOriginsThanRows() {
        expectCorrupt("invalid doc ref block", out -> {
            out.writeByte((byte) 0);
            out.writeVInt(2);
            out.writeVInt(3);
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

    private DocRefBlock build(List<DocRefOrigin> origins, boolean mayContainDuplicates, int[][] rows) {
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, rows.length)) {
            for (DocRefOrigin origin : origins) {
                builder.addOrigin(origin);
            }
            for (int[] row : rows) {
                builder.append(row[0], row[1], row[2]);
            }
            return builder.mayContainDuplicates(mayContainDuplicates).build();
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
                try (BlockStreamInput in = blockStreamInput(out); DocRefBlock read = DocRefBlock.readFrom(in)) {
                    fail("read a corrupt block " + read);
                }
            }
        });
        assertThat(e.getMessage(), containsString(message));
        assertThat("a rejected block leaves nothing on the breaker", blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
