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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;

public class StringColumnPlainPathChunkSizeTests extends ColumnarStringTestCase {

    private static final int SMALL_CHUNK = 32;
    private static final int LARGE_CHUNK = 1024 * 1024;

    public void testPlainPathUsesPlainPathTargetChunkBytes() throws IOException {
        final BytesRef[][] docSlots = uniqueDocSlots(200);
        assertEquals(2, plainChunkCount(docSlots, LARGE_CHUNK, SMALL_CHUNK));
        assertEquals(1, plainChunkCount(docSlots, LARGE_CHUNK, LARGE_CHUNK));
        assertEquals(1, plainChunkCount(docSlots, SMALL_CHUNK, LARGE_CHUNK));
    }

    public void testDictionaryPathUsesTargetChunkBytes() throws IOException {
        final BytesRef[][] docSlots = repeatedDocSlots(2000);
        assertEquals(2, dictionaryTermsChunkCount(docSlots, SMALL_CHUNK, LARGE_CHUNK));
        assertEquals(1, dictionaryTermsChunkCount(docSlots, LARGE_CHUNK, LARGE_CHUNK));
        assertEquals(1, dictionaryTermsChunkCount(docSlots, LARGE_CHUNK, SMALL_CHUNK));
    }

    private static BytesRef[][] uniqueDocSlots(int n) {
        final BytesRef[] docValues = new BytesRef[n];
        for (int i = 0; i < n; i++) {
            docValues[i] = new BytesRef("unique-value-padded-to-fill-chunks-" + i);
        }
        return singleValued(docValues);
    }

    private static BytesRef[][] repeatedDocSlots(int n) {
        final String[] terms = { "dictionary-term-alpha-padded-to-exceed-chunk-size", "dictionary-term-beta-padded-to-exceed-chunk-size" };
        final BytesRef[] docValues = new BytesRef[n];
        for (int i = 0; i < n; i++) {
            docValues[i] = new BytesRef(terms[i % terms.length]);
        }
        return singleValued(docValues);
    }

    private int plainChunkCount(BytesRef[][] docSlots, int targetChunkBytes, int plainPathTargetChunkBytes) throws IOException {
        final StringColumnMetadata metadata = writeColumn(docSlots, targetChunkBytes, plainPathTargetChunkBytes, DictionaryPolicy.NONE);
        final StringColumnMetadata.Plain plain = plainOf(metadata);
        return plain.values().chunks().numChunks();
    }

    private int dictionaryTermsChunkCount(BytesRef[][] docSlots, int targetChunkBytes, int plainPathTargetChunkBytes) throws IOException {
        final StringColumnMetadata metadata = writeColumn(
            docSlots,
            targetChunkBytes,
            plainPathTargetChunkBytes,
            StringColumnOptions.DEFAULT_DICTIONARY
        );
        final StringColumnMetadata.Dictionary dict = dictionaryOf(metadata);
        return dict.dictionary().chunks().numChunks();
    }

    private StringColumnMetadata writeColumn(
        BytesRef[][] docSlots,
        int targetChunkBytes,
        int plainPathTargetChunkBytes,
        DictionaryPolicy policy
    ) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata metadata;
            try (IndexOutput out = dir.createOutput("test.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, "ChunkSizeTest", FormatVersion.CURRENT, segmentId, "");
                metadata = StringColumnWriter.write(
                    docSlots.length,
                    numDocsWithField(docSlots),
                    numValues(docSlots),
                    numNullSlots(docSlots),
                    () -> cursor(docSlots),
                    ValueStream.VALUES_PER_BLOCK,
                    ChunkCodec.IDENTITY,
                    targetChunkBytes,
                    plainPathTargetChunkBytes,
                    policy,
                    null,
                    dir,
                    IOContext.DEFAULT,
                    out
                );
            }
            return metadata;
        }
    }
}
