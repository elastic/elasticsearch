/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.simdjson.internal.parsers.BitIndexes;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.simdjson.SimdJsonTestCase.buildBatchBuffer;
import static org.elasticsearch.simdjson.SimdJsonTestCase.computeLengths;
import static org.elasticsearch.simdjson.SimdJsonTestCase.computeOffsets;
import static org.elasticsearch.simdjson.SimdJsonTestCase.totalLen;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link SimdJsonParser}: stage 1 runs once over a contiguous buffer and
 * {@link SimdJsonParser#prepareDocumentWindow} provides per-document {@link BitIndexes} windows.
 */
public class SimdJsonParserTests extends SimdJsonTestCase {

    private static final int CAPACITY = 64 * 1024;

    // -- prepareDocumentWindow: stage 1 only, direct BitIndexes access --------

    // prepareDocumentWindow narrows BitIndexes to each document's byte range.
    public void testPrepareDocumentWindowSetsReadWindow() {
        String doc1 = "{\"a\":1}";
        String doc2 = "{\"b\":2}";
        byte[] buffer = buildBatchBuffer(doc1, doc2);
        int[] offsets = computeOffsets(doc1, doc2);
        int[] lengths = computeLengths(doc1, doc2);

        SimdJsonParser batch = newParser(CAPACITY);
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

    // Structural indices from doc 0 must not appear when walking doc 1.
    public void testPrepareDocumentWindowIsolatesDocuments() {
        String doc1 = "{\"a\":1,\"b\":2}";
        String doc2 = "{\"c\":3}";
        byte[] buffer = buildBatchBuffer(doc1, doc2);
        int[] offsets = computeOffsets(doc1, doc2);
        int[] lengths = computeLengths(doc1, doc2);

        SimdJsonParser batch = newParser(CAPACITY);
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

    // 100 small NDJSON docs in one batch — each window must start with '{'.
    public void testManySmallDocumentsWindow() {
        String[] docs = new String[100];
        for (int i = 0; i < 100; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);

        SimdJsonParser batch = newParser(CAPACITY);
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

    // prepareDocumentWindow before stage1 is an illegal state.
    public void testPrepareDocumentWindowBeforeStage1Throws() {
        SimdJsonParser batch = newParser(CAPACITY);
        expectThrows(IllegalStateException.class, () -> batch.prepareDocumentWindow(0, 10));
    }

    // -- batch reuse (stage1 called again) -----------------------------------

    // Calling stage1 again on a new buffer must replace the prior batch.
    public void testBatchReuse() {
        SimdJsonParser batch = newParser(CAPACITY);

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

    // stage1(offset, len) produces absolute structural indices in a padded buffer.
    public void testStage1WithOffset() {
        String json = "{\"a\":1}";
        byte[] raw = json.getBytes(UTF_8);
        int offset = 32;
        byte[] buffer = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buffer, offset, raw.length);

        SimdJsonParser batch = newParser(CAPACITY);
        batch.stage1(buffer, offset, raw.length);

        batch.prepareDocumentWindow(offset, raw.length);
        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        int firstIdx = bi.getAndAdvance();
        assertTrue("structural index should be absolute (>= offset)", firstIdx >= offset);
        assertEquals('{', buffer[firstIdx]);
    }

    // -- docLen exceeding chunk limit is rejected ------------------------------

    // Single document longer than CHUNK_BYTE_LIMIT is rejected up front.
    public void testChunkedRejectsDocLargerThanChunkLimit() {
        int oversized = SimdJsonParser.CHUNK_BYTE_LIMIT + 1;
        SimdJsonParser batch = newParser(oversized);
        byte[] buffer = new byte[oversized];
        batch.beginBatch(buffer, oversized);
        var e = expectThrows(IllegalArgumentException.class, () -> batch.prepareDocumentWindowChunked(0, oversized));
        assertThat(e.getMessage(), containsString("exceeds CHUNK_BYTE_LIMIT"));
    }

    // -- chunked single chunk -------------------------------------------------

    // Batch smaller than one chunk — all docs walk via prepareDocumentWindowChunked.
    public void testPrepareDocumentWindowChunkedSingleChunk() {
        String[] docs = new String[50];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);
        assertTrue("batch must fit in single chunk", total < SimdJsonParser.CHUNK_BYTE_LIMIT);

        SimdJsonParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < docs.length; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            assertEquals('{', buffer[bi.getAndAdvance()]);
        }
    }

    // -- chunked multiple chunks ----------------------------------------------

    // Chunk slice crosses document boundary — native stage1 rejects invalid JSON fragment.
    public void testPrepareDocumentWindowChunkedMultipleChunksRejectsInvalidBatchSlice() {
        String doc = "{\"i\":0}";
        int docLen = doc.getBytes(UTF_8).length;
        int docsNeeded = (SimdJsonParser.CHUNK_BYTE_LIMIT / docLen) + 100;
        String[] docs = new String[docsNeeded];
        for (int i = 0; i < docsNeeded; i++) {
            docs[i] = "{\"i\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);
        assertTrue("batch must exceed CHUNK_BYTE_LIMIT", total > SimdJsonParser.CHUNK_BYTE_LIMIT);

        SimdJsonParser batch = newParser(total);
        batch.beginBatch(buffer, total);

        expectThrows(JsonParsingException.class, () -> batch.prepareDocumentWindowChunked(offsets[0], lengths[0]));
    }

    // -- chunked doc at exact chunk boundary ----------------------------------

    // Document starting exactly at a chunk boundary must index correctly.
    public void testPrepareDocumentWindowChunkedDocAtExactChunkBoundary() {
        int chunkLimit = SimdJsonParser.CHUNK_BYTE_LIMIT;
        String smallDoc = "{\"x\":1}";
        int smallDocLen = smallDoc.getBytes(UTF_8).length;
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

        SimdJsonParser batch = newParser(total);
        batch.beginBatch(buffer, total);

        batch.prepareDocumentWindowChunked(offsets[docsInFirstChunk], lengths[docsInFirstChunk]);
        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked many small docs ----------------------------------------------

    // 1000 small docs spanning multiple chunks — each window must be valid.
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

        SimdJsonParser batch = newParser(total);
        batch.beginBatch(buffer, total);

        for (int d = 0; d < docCount; d++) {
            batch.prepareDocumentWindowChunked(offsets[d], lengths[d]);
            BitIndexes bi = batch.bitIndexes();
            assertFalse(bi.isEnd());
            assertEquals('{', buffer[bi.getAndAdvance()]);
        }
    }

    // -- beginBatch sub-range -------------------------------------------------

    // beginBatch(offset, len) indexes a sub-range inside a larger buffer.
    public void testBeginBatchSubRange() {
        String doc = "{\"sub\":true}";
        byte[] raw = doc.getBytes(UTF_8);
        int offset = 50;
        byte[] buffer = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buffer, offset, raw.length);

        SimdJsonParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, offset, raw.length);
        batch.prepareDocumentWindowChunked(offset, raw.length);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        int firstIdx = bi.getAndAdvance();
        assertTrue("structural index should be absolute (>= offset)", firstIdx >= offset);
        assertEquals('{', buffer[firstIdx]);
    }

    // -- chunked minimal doc --------------------------------------------------

    // Minimal {} document through the chunked API.
    public void testPrepareDocumentWindowMinimalDoc() {
        String doc = "{}";
        byte[] buffer = buildBatchBuffer(doc);
        int total = doc.getBytes(UTF_8).length;

        SimdJsonParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);
        batch.prepareDocumentWindowChunked(0, total);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked single field doc ---------------------------------------------

    // Single-field object through the chunked API.
    public void testPrepareDocumentWindowSingleFieldDoc() {
        String doc = "{\"a\":1}";
        byte[] buffer = buildBatchBuffer(doc);
        int total = doc.getBytes(UTF_8).length;

        SimdJsonParser batch = newParser(CAPACITY);
        batch.beginBatch(buffer, total);
        batch.prepareDocumentWindowChunked(0, total);

        BitIndexes bi = batch.bitIndexes();
        assertFalse(bi.isEnd());
        assertEquals('{', buffer[bi.getAndAdvance()]);
    }

    // -- chunked batch reuse --------------------------------------------------

    // beginBatch + prepareDocumentWindowChunked can be reused across buffers.
    public void testChunkedBatchReuse() {
        SimdJsonParser batch = newParser(CAPACITY);

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

    // Sentinel bytes restored between consecutive chunked document windows.
    public void testSentinelRestoreAcrossChunkedWindows() {
        String[] docs = new String[5];
        for (int i = 0; i < 5; i++) {
            docs[i] = "{\"v\":" + i + "}";
        }
        byte[] buffer = buildBatchBuffer(docs);
        int[] offsets = computeOffsets(docs);
        int[] lengths = computeLengths(docs);
        int total = totalLen(lengths);

        SimdJsonParser batch = newParser(CAPACITY);
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

    // Document offsets within a batch must be prepared in ascending order.
    public void testOutOfOrderDocumentWindowThrows() throws Exception {
        String first = "{\"first\":1}";
        String second = "{\"second\":2}";
        byte[] buffer = buildBatchBuffer(first, second);
        int[] offsets = computeOffsets(first, second);
        int[] lengths = computeLengths(first, second);

        SimdJsonParser parser = newParser(CAPACITY);
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(table.makeChild());
        parser.beginBatch(buffer, buffer.length);

        parser.prepareDocumentWindowChunked(offsets[1], lengths[1]);
        RecordingHandler handler = new RecordingHandler();
        walker.walkDocument(buffer, parser, handler);
        assertEquals(List.of("long(second=2,fitsInt=true)"), handler.events);

        parser.prepareDocumentWindowChunked(offsets[0], lengths[0]);
        handler.events.clear();
        expectThrows(JsonParsingException.class, () -> walker.walkDocument(buffer, parser, handler));
    }

    // Second stage1 on the same parser must not leak prior document structurals.
    public void testSecondStage1DoesNotLeakPriorDocument() throws Exception {
        String doc1 = "{\"only\":1}";
        String doc2 = "{\"other\":2}";
        List<String> baseline2 = walkJson(doc2);

        SimdJsonParser parser = newParser(CAPACITY);
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(table.makeChild());

        byte[] buf1 = doc1.getBytes(UTF_8);
        parser.stage1(buf1, buf1.length);
        parser.prepareDocumentWindow(0, buf1.length);
        walker.walkDocument(buf1, parser, new RecordingHandler());

        byte[] buf2 = doc2.getBytes(UTF_8);
        parser.stage1(buf2, buf2.length);
        parser.prepareDocumentWindow(0, buf2.length);
        RecordingHandler handler = new RecordingHandler();
        walker.walkDocument(buf2, parser, handler);

        assertEquals(baseline2, handler.events);
    }
}
