/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.lessThan;

/**
 * Verifies that {@link StringColumnOptions#DEFAULT_PLAIN_PATH_TARGET_CHUNK_BYTES} (512 KB) produces
 * materially smaller output than 64 KB on the plain path for high-cardinality URL-like data.
 *
 * <p>The plain path writes values in document order and is never bisected, so a larger chunk gives
 * ZSTD a wider context window without increasing read amplification. URL values sharing a long
 * common prefix compress significantly better when more of them fit into one chunk.
 */
public class StringColumnPlainPathChunkSizeTests extends ColumnarStringTestCase {

    private static final String PREFIX = "https://www.example.com/search?q=";
    private static final int N = 10_000;

    public void testLargerChunkProducesSmallerOutputForHighCardinalityUrls() throws IOException {
        final BytesRef[][] docSlots = urlDocSlots();
        final long smallChunkBytes = plainPathWriteSize(docSlots, 64 * 1024);
        final long largeChunkBytes = plainPathWriteSize(docSlots, StringColumnOptions.DEFAULT_PLAIN_PATH_TARGET_CHUNK_BYTES);
        assertThat(
            "512 KB plain-path chunks should compress URL data better than 64 KB chunks",
            largeChunkBytes,
            lessThan(smallChunkBytes)
        );
    }

    public void testRoundTripWithDefaultPlainPathChunkSize() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 500)];
        for (int i = 0; i < docValues.length; i++) {
            docValues[i] = new BytesRef(PREFIX + i + "/" + randomAlphaOfLength(8));
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            ChunkCodec.ZSTD,
            StringColumnOptions.DEFAULT_TARGET_CHUNK_BYTES,
            (metadata, reader) -> {
                plainOf(metadata);
                final ColumnIterator iterator = reader.iterator();
                for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    final int rank = iterator.rank();
                    assertEquals("doc " + doc, docValues[doc], reader.valueAt(reader.firstValueAddress(rank)));
                }
            }
        );
    }

    private BytesRef[][] urlDocSlots() {
        final BytesRef[] docValues = new BytesRef[N];
        for (int i = 0; i < N; i++) {
            docValues[i] = new BytesRef(PREFIX + randomAlphaOfLength(20));
        }
        return singleValued(docValues);
    }

    private long plainPathWriteSize(final BytesRef[][] docSlots, int plainPathTargetChunkBytes) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("plain.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, "PlainPathSizeTest", FormatVersion.CURRENT, segmentId, "");
                StringColumnWriter.write(
                    docSlots.length,
                    numDocsWithField(docSlots),
                    numValues(docSlots),
                    numNullSlots(docSlots),
                    () -> cursor(docSlots),
                    ValueStream.VALUES_PER_BLOCK,
                    ChunkCodec.ZSTD,
                    StringColumnOptions.DEFAULT_TARGET_CHUNK_BYTES,
                    plainPathTargetChunkBytes,
                    DictionaryPolicy.NONE,
                    null,
                    dir,
                    IOContext.DEFAULT,
                    out
                );
                return out.getFilePointer();
            }
        }
    }
}
