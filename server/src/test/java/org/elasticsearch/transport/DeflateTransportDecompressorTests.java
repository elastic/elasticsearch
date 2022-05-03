/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;

public class DeflateTransportDecompressorTests extends ESTestCase {

    private final Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    public void testSimpleCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            byte randomByte = randomByte();
            try (OutputStream deflateStream = CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(output))) {
                deflateStream.write(randomByte);
            }

            BytesReference bytes = output.bytes();

            DeflateTransportDecompressor decompressor = new DeflateTransportDecompressor(recycler);
            int bytesConsumed = decompressor.decompress(bytes);
            assertEquals(bytes.length(), bytesConsumed);
            assertTrue(decompressor.isEOS());
            ReleasableBytesReference releasableBytesReference = decompressor.pollDecompressedPage(true);
            assertEquals(randomByte, releasableBytesReference.get(0));
            releasableBytesReference.close();

        }
    }

    public void testMultiPageCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                StreamOutput deflateStream = new OutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(output))
                )
            ) {
                for (int i = 0; i < 10000; ++i) {
                    deflateStream.writeInt(i);
                }
            }

            BytesReference bytes = output.bytes();

            DeflateTransportDecompressor decompressor = new DeflateTransportDecompressor(recycler);
            int bytesConsumed = decompressor.decompress(bytes);
            assertEquals(bytes.length(), bytesConsumed);
            assertTrue(decompressor.isEOS());
            ReleasableBytesReference reference1 = decompressor.pollDecompressedPage(false);
            ReleasableBytesReference reference2 = decompressor.pollDecompressedPage(false);
            ReleasableBytesReference reference3 = decompressor.pollDecompressedPage(true);
            assertNull(decompressor.pollDecompressedPage(true));
            BytesReference composite = CompositeBytesReference.of(reference1, reference2, reference3);
            assertEquals(4 * 10000, composite.length());
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < 10000; ++i) {
                assertEquals(i, streamInput.readInt());
            }
            Releasables.close(reference1, reference2, reference3);
        }
    }

    public void testIncrementalMultiPageCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                StreamOutput deflateStream = new OutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(output))
                )
            ) {
                for (int i = 0; i < 10000; ++i) {
                    deflateStream.writeInt(i);
                }
            }

            BytesReference bytes = output.bytes();

            DeflateTransportDecompressor decompressor = new DeflateTransportDecompressor(recycler);

            int split1 = (int) (bytes.length() * 0.3);
            int split2 = (int) (bytes.length() * 0.65);
            BytesReference inbound1 = bytes.slice(0, split1);
            BytesReference inbound2 = bytes.slice(split1, split2 - split1);
            BytesReference inbound3 = bytes.slice(split2, bytes.length() - split2);

            int bytesConsumed1 = decompressor.decompress(inbound1);
            assertEquals(inbound1.length(), bytesConsumed1);
            assertFalse(decompressor.isEOS());
            int bytesConsumed2 = decompressor.decompress(inbound2);
            assertEquals(inbound2.length(), bytesConsumed2);
            assertFalse(decompressor.isEOS());
            int bytesConsumed3 = decompressor.decompress(inbound3);
            assertEquals(inbound3.length(), bytesConsumed3);
            assertTrue(decompressor.isEOS());
            ReleasableBytesReference reference1 = decompressor.pollDecompressedPage(false);
            ReleasableBytesReference reference2 = decompressor.pollDecompressedPage(false);
            ReleasableBytesReference reference3 = decompressor.pollDecompressedPage(true);
            assertNull(decompressor.pollDecompressedPage(false));
            BytesReference composite = CompositeBytesReference.of(reference1, reference2, reference3);
            assertEquals(4 * 10000, composite.length());
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < 10000; ++i) {
                assertEquals(i, streamInput.readInt());
            }
            Releasables.close(reference1, reference2, reference3);

        }
    }
}
