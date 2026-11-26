/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

public class SimdJsonParserPoolTests extends SimdJsonTestCase {

    private static final int MAX_DOC_BYTES = 4096;

    private SimdJsonParserPool newPool() {
        return new SimdJsonParserPool(MAX_DOC_BYTES);
    }

    public void testParseDocumentMatchesDirectWalk() {
        String json = "{\"a\":1,\"b\":\"x\"}";
        byte[] buffer = json.getBytes(UTF_8);

        RecordingHandler handler = new RecordingHandler(false);
        newPool().forCurrentThread().parseDocument(buffer, buffer.length, handler);

        assertEquals(walkJson(json), handler.events);
    }

    public void testParseDocumentWithNonZeroOffset() {
        String json = "{\"k\":42}";
        byte[] buffer = ("  " + json + "  ").getBytes(UTF_8);

        RecordingHandler handler = new RecordingHandler(false);
        newPool().forCurrentThread().parseDocument(buffer, 2, json.length(), handler);

        assertEquals(walkJson(json), handler.events);
    }

    public void testTruncatedDocumentThrowsParsingException() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        byte[] truncated = "{\"a\":".getBytes(UTF_8);
        expectThrows(JsonParsingException.class, () -> docParser.parseDocument(truncated, truncated.length, new RecordingHandler(false)));
    }

    public void testTruncatedDocumentWalksStaleIndexesOfPreviousDocument() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        byte[] previous = "{\"a\":1,\"b\":2,\"c\":3}".getBytes(UTF_8);
        docParser.parseDocument(previous, previous.length, new RecordingHandler(false));

        byte[] truncated = "{\"s\":\"}}}}}}}}}}\",\"y\":".getBytes(UTF_8);
        RecordingHandler handler = new RecordingHandler(false);
        try {
            docParser.parseDocument(truncated, truncated.length, handler);
            fail("truncated document parsed as " + handler.events);
        } catch (JsonParsingException expected) {}
    }

    /**
     * Cutting a valid document short, at any byte, must always fail with {@link JsonParsingException}.
     * It must never parse successfully, and never fail with some other exception type.
     *
     * <p>The parser keeps its structural index array between documents, and only the part belonging
     * to the current document is overwritten. The walker must therefore stop at the end of the
     * current document's indexes. If it did not, a truncated document would read the previous
     * document's leftover indexes instead of failing, and could even appear to parse successfully.
     * To make that failure as visible as possible, each prefix is parsed right after a large,
     * unrelated document.
     */
    public void testEveryTruncatedPrefixIsRejectedOnReusedParser() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        StringBuilder warm = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            warm.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":{\"v\":[1,2,{}]}");
        }
        byte[] warmBytes = warm.append('}').toString().getBytes(UTF_8);

        for (String json : SimdJsonTestDocuments.exactBufferLengthDocuments()) {
            byte[] full = json.getBytes(UTF_8);
            // A prefix that still contains the closing brace of the top-level object is a complete document.
            int lastBrace = json.lastIndexOf('}');
            for (int len = 1; len <= lastBrace; len++) {
                docParser.parseDocument(warmBytes, warmBytes.length, new RecordingHandler(false));
                int prefixLen = len;
                RecordingHandler handler = new RecordingHandler(false);
                expectThrows(
                    JsonParsingException.class,
                    "prefix [" + new String(full, 0, len, UTF_8) + "] of [" + json + "] must be rejected",
                    () -> docParser.parseDocument(full, prefixLen, handler)
                );
            }
        }
    }

    // Content after the document within the passed length is rejected by parseDocument, whether the
    // document starts at 0 or at an offset, and the parser is still usable afterwards.
    public void testContentAfterDocumentRejectedAndParserStillUsable() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        byte[] valid = "{\"a\":1}".getBytes(UTF_8);

        for (String json : new String[] { "{\"a\":1}{\"b\":2}", "{\"a\":1} x", "{}}", "{} []" }) {
            for (int offset : new int[] { 0, 3 }) {
                byte[] padded = (" ".repeat(offset) + json + "   ").getBytes(UTF_8);
                JsonParsingException e = expectThrows(
                    JsonParsingException.class,
                    "[" + json + "] at offset " + offset + " must be rejected",
                    () -> docParser.parseDocument(padded, offset, json.length(), new RecordingHandler(false))
                );
                assertThat(e.getMessage(), containsString("Unexpected content after end of document"));

                RecordingHandler handler = new RecordingHandler(false);
                docParser.parseDocument(valid, valid.length, handler);
                assertEquals(walkJson("{\"a\":1}"), handler.events);
            }
        }

        // Bytes outside [offset, offset + len) belong to neighbouring data and are
        // not content after the document.
        byte[] batch = "{\"a\":1}{\"b\":2}".getBytes(UTF_8);
        RecordingHandler first = new RecordingHandler(false);
        docParser.parseDocument(batch, 0, 7, first);
        assertEquals(walkJson("{\"a\":1}"), first.events);
        RecordingHandler second = new RecordingHandler(false);
        docParser.parseDocument(batch, 7, 7, second);
        assertEquals(walkJson("{\"b\":2}"), second.events);
    }

    public void testParseDocumentRejectsOversizedDocument() {
        JsonDocumentParser docParser = newPool().forCurrentThread();
        int tooLarge = docParser.maxDocumentBytes() + 1;
        byte[] buffer = new byte[tooLarge];
        buffer[0] = '{';
        buffer[tooLarge - 1] = '}';

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> docParser.parseDocument(buffer, tooLarge, new RecordingHandler(false))
        );
        assertThat(e.getMessage(), containsString("exceeds maximum"));
    }

    /** A thread always gets the same parser, so its structural index buffer and name cache stay warm. */
    public void testSameParserForSameThread() {
        SimdJsonParserPool pool = newPool();
        assertSame("a thread must reuse its own parser", pool.forCurrentThread(), pool.forCurrentThread());
    }

    /** Different threads get different parsers, since a parser is not thread-safe. */
    public void testDistinctParserPerThread() throws Exception {
        SimdJsonParserPool pool = newPool();
        JsonDocumentParser onTestThread = pool.forCurrentThread();

        AtomicReference<JsonDocumentParser> onOtherThread = new AtomicReference<>();
        Thread other = new Thread(() -> onOtherThread.set(pool.forCurrentThread()));
        other.start();
        other.join();

        assertNotSame("threads must not share a parser", onTestThread, onOtherThread.get());
    }

    /**
     * Field names learned on one thread reach a parser created later on another thread through the
     * pool's shared table: the second thread gets the identical String instance for the same name.
     */
    public void testFieldNamesAreSharedAcrossThreads() throws Exception {
        SimdJsonParserPool pool = newPool();
        byte[] buffer = "{\"shared_field_name\":1}".getBytes(UTF_8);

        // Parsing one document freezes this thread's name cache and publishes it to the shared table.
        NameCapturingHandler first = new NameCapturingHandler();
        JsonDocumentParser docParser = pool.forCurrentThread();
        docParser.parseDocument(buffer, buffer.length, first);
        docParser.publishFieldNames();

        NameCapturingHandler second = new NameCapturingHandler();
        Thread other = new Thread(() -> pool.forCurrentThread().parseDocument(buffer, buffer.length, second));
        other.start();
        other.join();

        assertEquals("shared_field_name", first.name);
        assertSame("field name must come from the pool's shared table", first.name, second.name);
    }

    /** Records the field name instance handed to the handler, to check name canonicalization. */
    private static class NameCapturingHandler extends RecordingHandler {
        volatile String name;

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            super.longField(fieldName, value, fitsInt, srcBuf, srcOff, srcLen);
            name = fieldName;
        }
    }
}
