/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Reads every slot of a plain column's values back in order, in reverse and at random: runs of equal values that
 * cross blocks of values and blocks of lengths, nulls, empty values, and values that straddle chunks.
 */
public class PlainValuesTests extends ESTestCase {

    private static final byte[] SEGMENT_ID = new byte[16];

    public void testEveryShapeReadsBack() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            final int valuesPerBlock = 1 << between(3, 9);
            final int lengthBlockSize = Math.max(128, valuesPerBlock << between(0, 3));
            final List<BytesRef> values = new ArrayList<>();
            final int numValues = between(1, 5000);
            while (values.size() < numValues) {
                final BytesRef value = switch (between(0, 9)) {
                    case 0 -> null;
                    case 1 -> new BytesRef("");
                    case 2 -> new BytesRef(randomAlphaOfLength(between(200, 3000)));
                    default -> new BytesRef(randomAlphaOfLength(between(1, 40)));
                };
                // Runs long enough to cross blocks of values and of lengths.
                for (int r = randomBoolean() ? 1 : between(1, 3 * lengthBlockSize); r > 0 && values.size() < numValues; r--) {
                    values.add(value);
                }
            }
            assertReadsBack(values, -1, valuesPerBlock, lengthBlockSize);
        }
    }

    public void testOneLengthReadsBack() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int length = iter == 0 ? 0 : between(0, 50);
            final int valuesPerBlock = 1 << between(3, 9);
            final List<BytesRef> values = new ArrayList<>();
            BytesRef value = new BytesRef(randomAlphaOfLength(length));
            for (int i = between(1, 5000); i > 0; i--) {
                if (randomInt(3) == 0) {
                    value = new BytesRef(randomAlphaOfLength(length));
                }
                values.add(value);
            }
            assertReadsBack(values, length, valuesPerBlock, Math.max(128, valuesPerBlock << between(0, 3)));
        }
    }

    /**
     * Values handed over a piece at a time, with the caller saying which repeat, read back by value and handed on in
     * pieces no larger than a chunk.
     */
    public void testTransferredValuesReadBack() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final ChunkBounds bounds = new ChunkBounds(between(16, 4096), randomBoolean() ? Integer.MAX_VALUE : between(1, 256));
            final List<BytesRef> values = new ArrayList<>();
            for (int i = between(1, 1000); i > 0; i--) {
                final int length = switch (between(0, 9)) {
                    case 0 -> -1;
                    case 1 -> 0;
                    case 2 -> between(bounds.targetBytes(), bounds.targetBytes() * 6);
                    case 3 -> values.isEmpty() || values.getLast() == null ? 1 : -2;
                    default -> between(1, 100);
                };
                values.add(switch (length) {
                    case -1 -> null;
                    case -2 -> BytesRef.deepCopyOf(values.getLast());
                    default -> new BytesRef(randomByteArrayOfLength(length));
                });
            }
            final ChunkCodec codec = randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
            final int valuesPerBlock = 1 << between(3, 9);
            final int lengthBlockSize = Math.max(128, valuesPerBlock);
            try (Directory dir = newDirectory()) {
                final PlainValues.Metadata metadata;
                long storedBytes = 0;
                try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "plain", SEGMENT_ID)) {
                    final PlainValues.Writer writer = new PlainValues.Writer(
                        codec,
                        bounds,
                        valuesPerBlock,
                        lengthBlockSize,
                        values.size(),
                        -1,
                        out.outputs()
                    );
                    final PiecewiseValue piecewise = new PiecewiseValue();
                    for (int i = 0; i < values.size(); i++) {
                        final BytesRef value = values.get(i);
                        if (value == null) {
                            writer.addNull();
                            continue;
                        }
                        final boolean repeat = i > 0 && values.get(i - 1) != null && values.get(i - 1).bytesEquals(value);
                        if (repeat == false || i % lengthBlockSize == 0) {
                            storedBytes += value.length;
                        }
                        piecewise.value = value;
                        writer.transfer(value.length, repeat, piecewise);
                    }
                    metadata = writer.finish();
                }
                assertEquals("a repeat is not stored again", storedBytes, metadata.chunks().uncompressedLength());
                assertEquals(bounds.targetBytes(), metadata.chunkTargetBytes());
                try (ColumnTestFiles.Inputs in = ColumnTestFiles.open(dir, "plain", SEGMENT_ID)) {
                    final PlainValues.Reader reader = metadata.open(in.inputs());
                    final BytesRef scratch = new BytesRef();
                    for (int probe = 0; probe < 2 * values.size(); probe++) {
                        final int i = probe < values.size() ? probe : between(0, values.size() - 1);
                        final BytesRef expected = values.get(i);
                        assertEquals(expected == null, reader.isNull(i));
                        if (expected == null) {
                            continue;
                        }
                        reader.get(i, scratch);
                        assertEquals("value " + i, expected, scratch);
                        final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        reader.transfer(i, (bytes, offset, length) -> {
                            // A chunk under a codec that decodes, and a bounded read of the file under one that does not.
                            assertThat(length, lessThanOrEqualTo(codec.isIdentity() ? 32 * 1024 : bounds.targetBytes()));
                            out.write(bytes, offset, length);
                        });
                        assertEquals("transferred " + i, expected, new BytesRef(out.toByteArray()));
                    }
                }
            }
        }
    }

    /**
     * A block holding more bytes than a chunk is not put back together whole: a short value in it is read on its own,
     * while a block of short values is still read as one span.
     */
    public void testABlockLargerThanAChunkIsReadAValueAtATime() throws IOException {
        final int chunk = 1024;
        final int valuesPerBlock = 128;
        final List<BytesRef> values = new ArrayList<>();
        // The first block: short values with a large one among them, together far larger than a chunk.
        for (int i = 0; i < valuesPerBlock; i++) {
            values.add(new BytesRef(randomByteArrayOfLength(i == 64 ? chunk * 20 : between(1, 8))));
        }
        // The second block: short values that fit a chunk together.
        for (int i = 0; i < valuesPerBlock; i++) {
            values.add(new BytesRef(randomByteArrayOfLength(between(1, 4))));
        }
        final ChunkCodec codec = randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
        try (Directory dir = newDirectory()) {
            final PlainValues.Metadata metadata;
            try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "plain", SEGMENT_ID)) {
                final PlainValues.Writer writer = new PlainValues.Writer(
                    codec,
                    new ChunkBounds(chunk, Integer.MAX_VALUE),
                    valuesPerBlock,
                    valuesPerBlock,
                    values.size(),
                    -1,
                    out.outputs()
                );
                for (BytesRef value : values) {
                    writer.add(value);
                }
                metadata = writer.finish();
            }
            try (ColumnTestFiles.Inputs in = ColumnTestFiles.open(dir, "plain", SEGMENT_ID)) {
                final PlainValues.Reader reader = metadata.open(in.inputs());
                final BytesRef scratch = new BytesRef();
                reader.get(3, scratch);
                assertEquals(values.get(3), scratch);
                assertThat("a short value in a large block", scratch.bytes.length, lessThanOrEqualTo(2 * chunk));
                reader.get(64, scratch);
                assertEquals(values.get(64), scratch);
                reader.get(valuesPerBlock + 3, scratch);
                assertEquals(values.get(valuesPerBlock + 3), scratch);
                final BytesRef again = new BytesRef();
                reader.get(valuesPerBlock + 4, again);
                assertEquals(values.get(valuesPerBlock + 4), again);
                assertSame("a block that fits a chunk is read as one span", scratch.bytes, again.bytes);
            }
        }
    }

    /** Hands its value over in small pieces of random size, and fails if anything asks for it whole. */
    private static final class PiecewiseValue extends StringColumnValues {
        BytesRef value;

        @Override
        public void transferValue(BytesSink sink) throws IOException {
            int at = 0;
            while (at < value.length) {
                final int take = Math.min(value.length - at, between(1, 64));
                sink.append(value.bytes, value.offset + at, take);
                at += take;
            }
        }

        @Override
        public BytesRef value() {
            throw new AssertionError("a transferred value is never asked for whole");
        }

        @Override
        public int valueCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nullCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void nextValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            throw new UnsupportedOperationException();
        }
    }

    private void assertReadsBack(List<BytesRef> values, int constantLength, int valuesPerBlock, int lengthBlockSize) throws IOException {
        final ChunkCodec codec = randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
        final ChunkBounds bounds = new ChunkBounds(between(16, 4096), randomBoolean() ? Integer.MAX_VALUE : between(1, 2048));
        final String label = "values=" + values.size() + " perBlock=" + valuesPerBlock + " lengthBlock=" + lengthBlockSize + " " + bounds;
        try (Directory dir = newDirectory()) {
            final PlainValues.Metadata metadata;
            final long expectedRuns;
            try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "plain", SEGMENT_ID)) {
                final PlainValues.Writer writer = new PlainValues.Writer(
                    codec,
                    bounds,
                    valuesPerBlock,
                    lengthBlockSize,
                    values.size(),
                    constantLength,
                    out.outputs()
                );
                long runs = 0;
                for (int i = 0; i < values.size(); i++) {
                    final BytesRef value = values.get(i);
                    if (i == 0 || isNewRun(values.get(i - 1), value)) {
                        runs++;
                    }
                    if (value == null) {
                        writer.addNull();
                    } else {
                        writer.add(value);
                    }
                }
                expectedRuns = runs;
                assertEquals(label + " runs", expectedRuns, writer.runs());
                metadata = writer.finish();
            }
            assertEquals(label, constantLength, metadata.constantLength());
            try (ColumnTestFiles.Inputs in = ColumnTestFiles.open(dir, "plain", SEGMENT_ID)) {
                final PlainValues.Reader reader = metadata.open(in.inputs());
                final int n = values.size();
                for (int i = 0; i < n; i++) {
                    assertSlot(label, reader, values, i);
                }
                for (int i = n - 1; i >= 0; i--) {
                    assertSlot(label, reader, values, i);
                }
                for (int probe = 0; probe < 2 * n; probe++) {
                    assertSlot(label, reader, values, between(0, n - 1));
                }
                // A repeat read right after the value it repeats answers with that value's stored bytes.
                final BytesRef scratch = new BytesRef();
                for (int i = 1; i < n; i++) {
                    final BytesRef previous = values.get(i - 1);
                    final BytesRef current = values.get(i);
                    if (previous == null || current == null) {
                        continue;
                    }
                    final long before = reader.read(i - 1, scratch);
                    final int beforeLength = scratch.length;
                    final long now = reader.read(i, scratch);
                    if (previous.bytesEquals(current) == false) {
                        assertFalse(label + " distinct values at " + i, before == now && beforeLength == scratch.length);
                    }
                }
            }
        }
    }

    private static boolean isNewRun(BytesRef previous, BytesRef current) {
        if (previous == null || current == null) {
            return previous != null || current != null;
        }
        return previous.bytesEquals(current) == false;
    }

    private static void assertSlot(String label, PlainValues.Reader reader, List<BytesRef> values, int i) throws IOException {
        final BytesRef expected = values.get(i);
        assertEquals(label + " null at " + i, expected == null, reader.isNull(i));
        assertEquals(label + " length at " + i, expected == null ? 0 : expected.length, reader.length(i));
        if (expected != null) {
            final BytesRef read = new BytesRef();
            reader.get(i, read);
            assertEquals(label + " value at " + i, expected, read);
        }
    }
}
