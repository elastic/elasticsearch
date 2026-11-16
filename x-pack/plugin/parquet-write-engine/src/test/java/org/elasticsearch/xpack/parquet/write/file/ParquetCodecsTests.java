/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.equalTo;

/**
 * The engine reads back the files that it writes (a compaction merges uploaded files), so every codec that it writes it must also read.
 * The other tests read the files with the codec factory of the Parquet reader plugin, which would hide a codec that our own factory
 * writes and cannot read.
 */
public class ParquetCodecsTests extends ESTestCase {

    public void testEveryCodecThatTheEngineWritesIsReadBack() throws Exception {
        final ParquetCodecs codecs = new ParquetCodecs();
        for (CompressionCodecName codec : new CompressionCodecName[] {
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD }) {
            // compressible bytes with some noise, as a column page is
            final byte[] original = new byte[randomIntBetween(1, 200_000)];
            for (int i = 0; i < original.length; i++) {
                original[i] = (byte) (i % 17 == 0 ? randomInt(255) : i % 5);
            }

            final byte[] compressed = codecs.getCompressor(codec).compress(BytesInput.from(original)).toByteArray();
            assertThat(
                codec.name(),
                codecs.getDecompressor(codec).decompress(BytesInput.from(compressed), original.length).toByteArray(),
                equalTo(original)
            );

            final ByteBuffer output = ByteBuffer.allocate(original.length);
            codecs.getDecompressor(codec).decompress(ByteBuffer.wrap(compressed), compressed.length, output, original.length);
            assertThat(codec.name(), output.array(), equalTo(original));
        }
    }
}
