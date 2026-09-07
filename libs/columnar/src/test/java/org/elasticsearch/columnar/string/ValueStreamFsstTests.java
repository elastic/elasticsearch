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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.lessThan;

public class ValueStreamFsstTests extends ESTestCase {

    private static final String FILE = "fsst.bin";

    public void testStructuredStringsRoundTrip() throws IOException {
        assertRoundTrip(structuredValues(500));
    }

    public void testHexIdsDoNotUseFsst() throws IOException {
        final List<BytesRef> values = hexIdValues(500, 32);
        assertEquals(
            "hex IDs should fall back to packed: FSST-enabled and FSST-disabled columns must match",
            writeWithoutFsst(values, ChunkCodec.IDENTITY),
            write(values, ChunkCodec.IDENTITY)
        );
    }

    public void testUuidsRoundTrip() throws IOException {
        assertRoundTrip(uuidValues(500));
    }

    public void testHexIdsRoundTrip() throws IOException {
        assertRoundTrip(hexIdValues(500, 32));
    }

    public void testFsstEncodingIsChosen() throws IOException {
        final List<BytesRef> values = structuredValues(500);
        assertThat(
            "structured strings with FSST should occupy fewer bytes than without FSST",
            write(values, ChunkCodec.IDENTITY),
            lessThan(writeWithoutFsst(values, ChunkCodec.IDENTITY))
        );
    }

    public void testHighEntropyValuesFallBackToPacked() throws IOException {
        final List<BytesRef> values = highEntropyValues(300);
        assertEquals(
            "high-entropy values should fall back to packed: FSST-enabled and FSST-disabled columns must match",
            writeWithoutFsst(values, ChunkCodec.IDENTITY),
            write(values, ChunkCodec.IDENTITY)
        );
    }

    public void testShortValuesUseInlineLayout() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add(new BytesRef("short-" + i));
        }
        assertEquals(
            "short values should use INLINE layout: FSST-enabled and FSST-disabled columns must match",
            writeWithoutFsst(values, ChunkCodec.IDENTITY),
            write(values, ChunkCodec.IDENTITY)
        );
    }

    public void testMixedFsstAndInlineBlocks() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            values.add(new BytesRef("short-" + i));
        }
        for (int i = 0; i < 300; i++) {
            values.add(new BytesRef("kubernetes-node-" + i + ".us-east-1.compute.internal"));
        }
        assertRoundTrip(values);
    }

    public void testWeakTableFallsBackToPacked() throws IOException {
        final byte[] bytes = new byte[64];
        bytes[0] = 'z';
        bytes[1] = 'z';
        bytes[30] = 'z';
        bytes[31] = 'z';
        int fill = 0;
        for (int i = 0; i < 64; i++) {
            if (bytes[i] == 0) {
                bytes[i] = (byte) fill++;
            }
        }
        final List<BytesRef> values = new ArrayList<>();
        values.add(new BytesRef(bytes));
        assertEquals(
            "one-symbol FSST table should fall back to packed: FSST-enabled and FSST-disabled must match",
            writeWithoutFsst(values, ChunkCodec.IDENTITY),
            write(values, ChunkCodec.IDENTITY)
        );
    }

    private long write(List<BytesRef> values, ChunkCodec codec) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        codec,
                        65536,
                        128,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "fsst",
                        out,
                        new FsstBlockCodecBuilder().build()
                    )
                ) {
                    for (final BytesRef value : values) {
                        writer.add(value);
                    }
                    writer.finish();
                }
            }
            return dir.fileLength(FILE);
        }
    }

    private List<BytesRef> structuredValues(int count) {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(new BytesRef("kubernetes-node-" + i + ".us-east-1.compute.internal"));
        }
        return values;
    }

    private long writeWithoutFsst(List<BytesRef> values, ChunkCodec codec) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                try (
                    ValueStream.Writer writer = new ValueStream.Writer(
                        codec,
                        65536,
                        128,
                        values.size(),
                        dir,
                        IOContext.DEFAULT,
                        "fsst",
                        out
                    )
                ) {
                    for (final BytesRef value : values) {
                        writer.add(value);
                    }
                    writer.finish();
                }
            }
            return dir.fileLength(FILE);
        }
    }

    private List<BytesRef> hexIdValues(int count, int valueLen) {
        final byte[] hexChars = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
        final List<BytesRef> values = new ArrayList<>();
        for (int v = 0; v < count; v++) {
            final byte[] bytes = new byte[valueLen];
            for (int i = 0; i < valueLen; i++) {
                bytes[i] = hexChars[random().nextInt(16)];
            }
            values.add(new BytesRef(bytes));
        }
        return values;
    }

    private List<BytesRef> uuidValues(int count) {
        final byte[] hexChars = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
        final List<BytesRef> values = new ArrayList<>();
        for (int v = 0; v < count; v++) {
            final byte[] bytes = new byte[36];
            for (int i = 0; i < 36; i++) {
                if (i == 8 || i == 13 || i == 18 || i == 23) {
                    bytes[i] = '-';
                } else {
                    bytes[i] = hexChars[random().nextInt(16)];
                }
            }
            values.add(new BytesRef(bytes));
        }
        return values;
    }

    private List<BytesRef> highEntropyValues(int count) {
        final List<BytesRef> values = new ArrayList<>();
        final byte[] all256 = new byte[256];
        for (int b = 0; b < 256; b++) {
            all256[b] = (byte) b;
        }
        values.add(new BytesRef(all256));
        for (int i = 1; i < count; i++) {
            values.add(new BytesRef(randomByteArrayOfLength(between(32, 64))));
        }
        return values;
    }

    private void assertRoundTrip(List<BytesRef> values) throws IOException {
        for (final int perBlock : new int[] { 8, 128, 512 }) {
            try (Directory dir = newDirectory()) {
                final ValueStream.Metadata metadata;
                try (IndexOutput out = dir.createOutput(FILE, IOContext.DEFAULT)) {
                    try (
                        ValueStream.Writer writer = new ValueStream.Writer(
                            randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD),
                            randomFrom(64, 4096, 65536),
                            perBlock,
                            values.size(),
                            dir,
                            IOContext.DEFAULT,
                            "fsst",
                            out,
                            new FsstBlockCodecBuilder().build()
                        )
                    ) {
                        for (final BytesRef value : values) {
                            writer.add(value);
                        }
                        metadata = writer.finish();
                    }
                }
                try (IndexInput in = dir.openInput(FILE, IOContext.DEFAULT)) {
                    final ValueStream.Reader reader = metadata.open(in);
                    final String label = "perBlock=" + perBlock;
                    final BytesRef read = new BytesRef();
                    for (int i = 0; i < values.size(); i++) {
                        reader.get(i, read);
                        assertEquals(label + " at " + i, values.get(i), read);
                    }
                    for (int i = values.size() - 1; i >= 0; i--) {
                        reader.get(i, read);
                        assertEquals(label + " backwards at " + i, values.get(i), read);
                    }
                    for (int probe = 0; probe < 50; probe++) {
                        final int i = between(0, values.size() - 1);
                        reader.get(i, read);
                        assertEquals(label + " random at " + i, values.get(i), read);
                    }
                }
            }
        }
    }
}
