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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;

/**
 * The byte sequence a string column stores its values in, on its own: whatever is written reads back, at
 * every block layout and every bit width a packed length takes.
 */
public class ValueStreamTests extends ESTestCase {

    private static final String FILE = "stream.bin";

    /** Values short enough that a block keeps each length beside its own value with the ANY layout. */
    public void testInlineLengths() throws IOException {
        assertRoundTrip(values(between(200, 2000), 0, 20));
    }

    /** Values of length 0 and 1, so the packed header is 1 bit per value. */
    public void testPackedOneBit() throws IOException {
        assertRoundTrip(values(between(200, 2000), 0, 1));
    }

    /** Values up to 255 bytes, so the packed header is at most 8 bits per value. */
    public void testPackedEightBits() throws IOException {
        assertRoundTrip(values(between(200, 1500), 40, 255));
    }

    /** Values up to 4095 bytes, so the packed header is at most 12 bits per value. */
    public void testPackedTwelveBits() throws IOException {
        assertRoundTrip(values(between(100, 600), 300, 4000));
    }

    /** Values whose length needs 17 bits, past what two bytes could hold. */
    public void testPackedSeventeenBits() throws IOException {
        assertRoundTrip(values(between(4, 20), 66_000, 66_500));
    }

    /** A column that mixes them, so the layout differs from one block to the next. */
    public void testLayoutVariesBetweenBlocks() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0, count = between(500, 3000); i < count; i++) {
            values.add(new BytesRef(randomAlphaOfLengthBetween(1, randomBoolean() ? 8 : 400)));
        }
        assertRoundTrip(values);
    }

    /** Nothing written at all. */
    public void testEmptyStream() throws IOException {
        assertRoundTrip(List.of());
    }

    /** Every value empty, so the stream holds no bytes and every read is a read of nothing. */
    public void testAllValuesEmpty() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0, count = between(1, 500); i < count; i++) {
            values.add(new BytesRef(""));
        }
        assertRoundTrip(values);
    }

    /** Empty values among others, where a length of zero has to be told apart from the value beside it. */
    public void testEmptyValuesAmongOthers() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0, count = between(200, 2000); i < count; i++) {
            values.add(randomBoolean() ? new BytesRef("") : new BytesRef(randomAlphaOfLengthBetween(1, 50)));
        }
        assertRoundTrip(values);
    }

    /** A value larger than the bytes a chunk holds, which a chunk has to grow past rather than split. */
    public void testValueLargerThanAChunk() throws IOException {
        final List<BytesRef> values = values(between(4, 30), 600, 1200);
        assertRoundTrip(values, randomFrom(8, 32, 128), randomChunkCodec(), 256, randomLayouts());
    }

    /**
     * A stream restricted to contiguous values never writes the inline layout, even when the values are
     * short enough that a stream set to {@link ValueStream.Layouts#ANY} would choose inline. The on-disk
     * size with an identity codec (no compression) confirms packed was chosen: packed at a few bits per
     * value uses a shorter header than one vint per value.
     */
    public void testContiguousValuesNeverWritesInline() throws IOException {
        // Values short enough that ANY would choose inline (mean < INLINE_MEAN_LENGTH = 32).
        // CONTIGUOUS_VALUES must use packed instead.
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            values.add(new BytesRef(randomAlphaOfLength(15)));
        }
        assertRoundTrip(values, 128, ChunkCodec.IDENTITY, 1024 * 1024, ValueStream.Layouts.CONTIGUOUS_VALUES);

        // Packed at bitsRequired(15) = 4 bits writes 64 bytes of length header per 128-value block.
        // Inline writes ~128 vint bytes. The identity codec removes compression from the equation, so
        // the difference in file size is purely the layout — packed should be smaller.
        try (Directory dir = newDirectory()) {
            final long anyBytes;
            try (IndexOutput out = dir.createOutput("any.bin", IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        ChunkCodec.IDENTITY,
                        1024 * 1024,
                        128,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "x",
                        out,
                        ValueStream.Layouts.ANY
                    )
                ) {
                    for (BytesRef value : values)
                        writer.add(value);
                    writer.finish();
                }
            }
            anyBytes = dir.fileLength("any.bin");

            final long contigBytes;
            try (IndexOutput out = dir.createOutput("contig.bin", IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        ChunkCodec.IDENTITY,
                        1024 * 1024,
                        128,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "x",
                        out,
                        ValueStream.Layouts.CONTIGUOUS_VALUES
                    )
                ) {
                    for (BytesRef value : values)
                        writer.add(value);
                    writer.finish();
                }
            }
            contigBytes = dir.fileLength("contig.bin");

            assertThat(
                "packed (4 bits/value) should write a smaller header than inline (1 vint/value) for uniform 15-byte values",
                contigBytes,
                lessThan(anyBytes)
            );
        }
    }

    private static List<BytesRef> values(int count, int minLength, int maxLength) {
        final List<BytesRef> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(new BytesRef(minLength == 0 && maxLength == 0 ? "" : randomAlphaOfLengthBetween(Math.max(minLength, 1), maxLength)));
        }
        return values;
    }

    private static ChunkCodec randomChunkCodec() {
        return randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
    }

    private static ValueStream.Layouts randomLayouts() {
        return randomFrom(ValueStream.Layouts.values());
    }

    private void assertRoundTrip(List<BytesRef> values) throws IOException {
        assertRoundTrip(values, randomFrom(8, 32, 128, 512), randomChunkCodec(), randomFrom(64, 512, 4096, 64 * 1024), randomLayouts());
    }

    /** Writes the values, reads every one back in order, backwards, and at random. */
    private void assertRoundTrip(
        List<BytesRef> values,
        int valuesPerBlock,
        ChunkCodec codec,
        int targetChunkBytes,
        ValueStream.Layouts layouts
    ) throws IOException {
        final String label = "codec="
            + codec
            + " perBlock="
            + valuesPerBlock
            + " chunk="
            + targetChunkBytes
            + " layouts="
            + layouts
            + " n="
            + values.size();
        try (Directory dir = newDirectory()) {
            final ValueStream.Metadata metadata;
            try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        codec,
                        targetChunkBytes,
                        valuesPerBlock,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "stream",
                        out,
                        layouts
                    )
                ) {
                    for (BytesRef value : values) {
                        writer.add(value);
                    }
                    metadata = writer.finish();
                }
            }
            assertEquals(label + " numValues", values.size(), metadata.numValues());
            long valueBytes = 0;
            for (BytesRef value : values) {
                valueBytes += value.length;
            }
            assertEquals(label + " valueBytes", valueBytes, metadata.valueBytes());

            try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
                final ValueStream.Reader reader = metadata.open(in);
                final BytesRef read = new BytesRef();

                for (int i = 0; i < values.size(); i++) {
                    reader.get(i, read);
                    assertEquals(label + " in order at " + i, values.get(i), read);
                }
                // Backwards, so every read re-enters a block the reader has already left.
                for (int i = values.size() - 1; i >= 0; i--) {
                    reader.get(i, read);
                    assertEquals(label + " backwards at " + i, values.get(i), read);
                }
                for (int probe = 0; probe < Math.min(200, values.size()); probe++) {
                    final int i = between(0, values.size() - 1);
                    reader.get(i, read);
                    assertEquals(label + " random at " + i, values.get(i), read);
                }
            }
        }
    }

    /** Every value a block's first byte may take; anything else is a corrupt index. */
    public void testUnknownLayoutMarkersAreNotAccepted() {
        for (ValueStream.BlockLayout layout : ValueStream.BlockLayout.values()) {
            assertSame("id " + layout.id + " round-trips to " + layout, layout, ValueStream.BlockLayout.fromId(layout.id));
        }
        for (byte marker : new byte[] { 3, 4, 5, 6, 42, -1, Byte.MIN_VALUE, Byte.MAX_VALUE }) {
            assertNull("marker " + marker + " names no known layout", ValueStream.BlockLayout.fromId(marker));
        }
    }
}
