/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.BitIndexes;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.buildBatchBuffer;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.computeLengths;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.computeOffsets;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.newParser;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.totalLen;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link SimdJsonBatchParser}: stage 1 runs once over a contiguous buffer and
 * {@link SimdJsonBatchParser#prepareDocumentWindow} provides per-document {@link BitIndexes} windows.
 */
public class SimdJsonBatchParserTests extends ESTestCase {

    private static final int CAPACITY = 64 * 1024;

    // -- prepareDocumentWindow: stage 1 only, direct BitIndexes access --------

    public void testPrepareDocumentWindowSetsReadWindow() {
        String doc1 = "{\"a\":1}";
        String doc2 = "{\"b\":2}";
        byte[] buffer = buildBatchBuffer(doc1, doc2);
        int[] offsets = computeOffsets(doc1, doc2);
        int[] lengths = computeLengths(doc1, doc2);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.stage1(buffer, totalLen(lengths));

        batch.prepareDocumentWindow(offsets[0], lengths[0]);
        BitIndexes bi = batch.bitIndexes();
        assertFalse("BitIndexes should have entries for doc 0", bi.isEnd());
        int firstIdx = bi.getAndAdvance();
        assertEquals('{', buffer[firstIdx]);

        batch.prepareDocumentWindow(offsets[1], lengths[1]);
        assertFalse("BitIndexes should have entries for doc 1", bi.isEnd());
        int secondIdx = bi.getAndAdvance();
        assertEquals('{', buffer[secondIdx]);
        assertTrue("second doc starts at or after first doc ends", secondIdx >= offsets[1]);
    }

    public void testPrepareDocumentWindowIsolatesDocuments() {
        String doc1 = "{\"a\":1,\"b\":2}";
        String doc2 = "{\"c\":3}";
        byte[] buffer = buildBatchBuffer(doc1, doc2);
        int[] offsets = computeOffsets(doc1, doc2);
        int[] lengths = computeLengths(doc1, doc2);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.stage1(buffer, totalLen(lengths));

        batch.prepareDocumentWindow(offsets[0], lengths[0]);
        BitIndexes bi = batch.bitIndexes();
        int count = 0;
        while (!bi.isEnd()) {
            int idx = bi.getAndAdvance();
            assertTrue("structural index " + idx + " should be within doc 0 range", idx < offsets[0] + lengths[0]);
            count++;
        }
        assertTrue("doc 0 should have multiple structural indices", count > 1);

        batch.prepareDocumentWindow(offsets[1], lengths[1]);
        count = 0;
        while (!bi.isEnd()) {
            int idx = bi.getAndAdvance();
            assertTrue(
                "structural index " + idx + " should be within doc 1 range [" + offsets[1] + ", " + (offsets[1] + lengths[1]) + ")",
                idx >= offsets[1] && idx < offsets[1] + lengths[1]
            );
            count++;
        }
        assertTrue("doc 1 should have structural indices", count > 0);
    }

    public void testManySmallDocumentsWindow() {
        String[] docs = new String[100];
        for (int i = 0; i < 100; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.stage1(buffer, totalLen(lengths));

        for (int d = 0; d < docs.length; d++) {
            batch.prepareDocumentWindow(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse("BitIndexes should have entries for doc " + d, bi.isEnd());
            int firstIdx = bi.getAndAdvance();
            assertEquals("doc " + d + " should start with '{'", '{', buffer[firstIdx]);
        }
    }

    // -- stage1 must be called first -----------------------------------------

    public void testPrepareDocumentWindowBeforeStage1Throws() {
        SimdJsonBatchParser batch = newParser(CAPACITY);
        expectThrows(IllegalStateException.class, () -> batch.prepareDocumentWindow(0, 10));
    }

    // -- batch reuse (stage1 called again) -----------------------------------

    public void testBatchReuse() {
        SimdJsonBatchParser batch = newParser(CAPACITY);

        String[] docs1 = { "{\"a\":1}", "{\"b\":2}" };
        byte[] buffer1 = buildBatchBuffer(docs1);
        int[] offsets1 = computeOffsets(docs1);
        int[] lengths1 = computeLengths(docs1);

        batch.stage1(buffer1, totalLen(lengths1));
        for (int d = 0; d < docs1.length; d++) {
            batch.prepareDocumentWindow(offsets1[d], lengths1[d]);
            assertFalse(batch.bitIndexes().isEnd());
        }

        String[] docs2 = { "{\"x\":10,\"y\":20}", "{\"z\":30}" };
        byte[] buffer2 = buildBatchBuffer(docs2);
        int[] offsets2 = computeOffsets(docs2);
        int[] lengths2 = computeLengths(docs2);

        batch.stage1(buffer2, totalLen(lengths2));
        for (int d = 0; d < docs2.length; d++) {
            batch.prepareDocumentWindow(offsets2[d], lengths2[d]);
            assertFalse(batch.bitIndexes().isEnd());
        }
    }

    // -- stage1 with offset ---------------------------------------------------

    public void testStage1WithOffset() {
        String json = "{\"a\":1}";
        byte[] raw = json.getBytes(StandardCharsets.UTF_8);
        int offset = 32;
        byte[] buffer = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buffer, offset, raw.length);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.stage1(buffer, offset, raw.length);

        batch.prepareDocumentWindow(offset, raw.length);
        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        int firstIdx = bi.getAndAdvance();
        assertTrue("structural index should be absolute (>= offset)", firstIdx >= offset);
        assertEquals('{', buffer[firstIdx]);
    }

    // -- docLen exceeding chunk limit is rejected ------------------------------

    public void testChunkedRejectsDocLargerThanChunkLimit() {
        int oversized = SimdJsonBatchParser.CHUNK_BYTE_LIMIT + 1;
        SimdJsonBatchParser batch = newParser(oversized);
        byte[] buffer = new byte[oversized];
        batch.beginBatch(buffer, oversized);
        var e = expectThrows(IllegalArgumentException.class, () -> batch.prepareDocumentWindowChunked(0, oversized));
        assertThat(e.getMessage(), containsString("exceeds CHUNK_BYTE_LIMIT"));
    }

    // -- chunked single chunk -------------------------------------------------

    public void testPrepareDocumentWindowChunkedSingleChunk() {
        String[] docs = new String[50];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);
        assertTrue("batch must fit in single chunk", total < SimdJsonBatchParser.CHUNK_BYTE_LIMIT);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < docs.length; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            assertEquals('{', buffer[bi.getAndAdvance()]);
        }
    }

    // -- chunked multiple chunks ----------------------------------------------

    public void testPrepareDocumentWindowChunkedMultipleChunks() {
        String doc = "{\"i\":0}";
        int docLen = doc.getBytes(StandardCharsets.UTF_8).length;
        int docsNeeded = (SimdJsonBatchParser.CHUNK_BYTE_LIMIT / docLen) + 100;
        String[] docs = new String[docsNeeded];
        for (int i = 0; i < docsNeeded; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);
        assertTrue("batch must exceed CHUNK_BYTE_LIMIT", total > SimdJsonBatchParser.CHUNK_BYTE_LIMIT);

        SimdJsonBatchParser batch = new SimdJsonBatchParser(total, SimdJsonTestSupport::scalarStage1);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < docs.length; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse("doc " + d + " should have indices", bi.isEnd());
            assertEquals('{', buffer[bi.getAndAdvance()]);
        }
    }

    // -- chunked doc at exact chunk boundary ----------------------------------

    public void testPrepareDocumentWindowChunkedDocAtExactChunkBoundary() {
        int chunkLimit = SimdJsonBatchParser.CHUNK_BYTE_LIMIT;
        String smallDoc = "{\"x\":1}";
        int smallDocLen = smallDoc.getBytes(StandardCharsets.UTF_8).length;
        int docsInFirstChunk = chunkLimit / smallDocLen;
        int firstChunkActualSize = docsInFirstChunk * smallDocLen;

        int extraDocs = 10;
        int totalDocs = docsInFirstChunk + extraDocs;
        String[] docs = new String[totalDocs];
        for (int i = 0; i < totalDocs; i++) {
            docs[i] = smallDoc;
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);

        assertEquals("doc at boundary starts at first chunk size", firstChunkActualSize, offsets[docsInFirstChunk]);

        SimdJsonBatchParser batch = new SimdJsonBatchParser(total, SimdJsonTestSupport::scalarStage1);
        batch.beginBatch(buffer, total);

        batch.prepareDocumentWindowChunked(offsets[docsInFirstChunk], lengths[docsInFirstChunk]);
        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked many small docs ----------------------------------------------

    public void testChunkedBatchManySmallDocs() {
        int docCount = 1000;
        String[] docs = new String[docCount];
        for (int i = 0; i < docCount; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);

        SimdJsonBatchParser batch = new SimdJsonBatchParser(total, SimdJsonTestSupport::scalarStage1);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < docCount; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            assertEquals('{', buffer[bi.getAndAdvance()]);
        }
    }

    // -- beginBatch sub-range -------------------------------------------------

    public void testBeginBatchSubRange() {
        String doc = "{\"sub\":true}";
        byte[] raw = doc.getBytes(StandardCharsets.UTF_8);
        int offset = 50;
        byte[] buffer = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buffer, offset, raw.length);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, offset, raw.length);
        batch.prepareDocumentWindowChunked(offset, raw.length);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        int firstIdx = bi.getAndAdvance();
        assertTrue("structural index should be absolute (>= offset)", firstIdx >= offset);
        assertEquals('{', buffer[firstIdx]);
    }

    // -- chunked minimal doc --------------------------------------------------

    public void testPrepareDocumentWindowMinimalDoc() {
        String doc = "{}";
        byte[] buffer = buildBatchBuffer(doc);
        int total = doc.getBytes(StandardCharsets.UTF_8).length;

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);
        batch.prepareDocumentWindowChunked(0, total);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked single field doc ---------------------------------------------

    public void testPrepareDocumentWindowSingleFieldDoc() {
        String doc = "{\"a\":1}";
        byte[] buffer = buildBatchBuffer(doc);
        int total = doc.getBytes(StandardCharsets.UTF_8).length;

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);
        batch.prepareDocumentWindowChunked(0, total);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked batch reuse --------------------------------------------------

    public void testChunkedBatchReuse() {
        SimdJsonBatchParser batch = newParser(CAPACITY);

        String[] docs1 = { "{\"a\":1}", "{\"b\":2}" };
        byte[] buffer1 = buildBatchBuffer(docs1);
        int[] offsets1 = computeOffsets(docs1);
        int[] lengths1 = computeLengths(docs1);
        int total1 = totalLen(lengths1);

        batch.beginBatch(buffer1, total1);
        for (int d = 0; d < docs1.length; d++) {
            batch.prepareDocumentWindowChunked(offsets1[d], lengths1[d]);
            assertFalse(batch.bitIndexes().isEnd());
        }

        String[] docs2 = { "{\"x\":10}", "{\"y\":20}", "{\"z\":30}" };
        byte[] buffer2 = buildBatchBuffer(docs2);
        int[] offsets2 = computeOffsets(docs2);
        int[] lengths2 = computeLengths(docs2);
        int total2 = totalLen(lengths2);

        batch.beginBatch(buffer2, total2);
        for (int d = 0; d < docs2.length; d++) {
            batch.prepareDocumentWindowChunked(offsets2[d], lengths2[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            assertEquals('{', buffer2[bi.getAndAdvance()]);
        }
    }

    // -- sentinel restore across chunked windows ------------------------------

    public void testSentinelRestoreAcrossChunkedWindows() {
        String[] docs = new String[5];
        for (int i = 0; i < 5; i++) {
            docs[i] = "{\"v\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);

        SimdJsonBatchParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < 5; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            int firstIdx = bi.getAndAdvance();
            assertEquals('{', buffer[firstIdx]);
            assertTrue("index " + firstIdx + " within doc bounds", firstIdx >= offsets[d] && firstIdx < offsets[d] + lengths[d]);
        }
    }
}
