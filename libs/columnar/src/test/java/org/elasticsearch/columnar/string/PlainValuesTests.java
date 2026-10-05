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
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
