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
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared test infrastructure for simdjson tests. Provides a scalar (non-SIMD) stage 1
 * implementation, a recording {@link JsonDocumentHandler}, and buffer/batch helpers.
 */
public final class SimdJsonTestSupport {

    private SimdJsonTestSupport() {}

    // ---- Scalar stage 1 delegate (no SIMD, suitable for all platforms) ----

    /**
     * Walks bytes one at a time and records the position of each structural character
     * and the start of scalars. Sufficient for well-formed test inputs.
     */
    static void scalarStage1(byte[] buffer, int offset, int len, BitIndexes bitIndexes) {
        bitIndexes.ensureCapacity(len + 1);
        bitIndexes.reset();
        boolean inString = false;
        boolean prevBackslash = false;
        boolean prevScalar = false;
        int end = offset + len;
        int writeIdx = 0;
        int[] raw = bitIndexes.rawIndexes();
        for (int i = offset; i < end; i++) {
            byte b = buffer[i];
            if (inString) {
                if (prevBackslash) {
                    prevBackslash = false;
                } else if (b == '\\') {
                    prevBackslash = true;
                } else if (b == '"') {
                    inString = false;
                }
                continue;
            }
            boolean isStructural = b == '{' || b == '}' || b == '[' || b == ']' || b == ':' || b == ',';
            boolean isWhitespace = b == ' ' || b == '\t' || b == '\n' || b == '\r';
            if (b == '"') {
                if (!prevScalar) {
                    raw[writeIdx++] = i;
                }
                inString = true;
                prevScalar = true;
            } else if (isStructural) {
                raw[writeIdx++] = i;
                prevScalar = false;
            } else if (isWhitespace) {
                prevScalar = false;
            } else {
                if (!prevScalar) {
                    raw[writeIdx++] = i;
                }
                prevScalar = true;
            }
        }
        bitIndexes.setWriteIdx(writeIdx);
    }

    // ---- Recording handler ----

    /**
     * {@link JsonDocumentHandler} that records all events as strings in a list.
     * Two variants of {@link #emptyObject}: the default emits a single
     * {@code emptyObject(name)} event; pass {@code normalizeEmptyObject=true}
     * to the constructor to emit {@code startObject/endObject} instead (useful
     * when comparing against Jackson, which always emits start/end).
     */
    static class RecordingHandler implements JsonDocumentHandler {
        final List<String> events = new ArrayList<>();
        private final boolean normalizeEmptyObject;

        RecordingHandler() {
            this(false);
        }

        RecordingHandler(boolean normalizeEmptyObject) {
            this.normalizeEmptyObject = normalizeEmptyObject;
        }

        @Override
        public void startObject(String fieldName) {
            events.add("startObject(" + fieldName + ")");
        }

        @Override
        public void endObject() {
            events.add("endObject()");
        }

        @Override
        public void emptyObject(String fieldName) {
            if (normalizeEmptyObject) {
                events.add("startObject(" + fieldName + ")");
                events.add("endObject()");
            } else {
                events.add("emptyObject(" + fieldName + ")");
            }
        }

        @Override
        public void stringField(String fieldName, byte[] buf, int off, int len) {
            events.add("string(" + fieldName + "=" + new String(buf, off, len, StandardCharsets.UTF_8) + ")");
        }

        @Override
        public void longField(String fieldName, long value, boolean fitsInt, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("long(" + fieldName + "=" + value + ",fitsInt=" + fitsInt + ")");
        }

        @Override
        public void bigIntegerField(String fieldName, BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("bigInteger(" + fieldName + "=" + value + ")");
        }

        @Override
        public void doubleField(String fieldName, double value, boolean fitsFloat, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("double(" + fieldName + "=" + value + ",fitsFloat=" + fitsFloat + ")");
        }

        @Override
        public void booleanField(String fieldName, boolean value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("bool(" + fieldName + "=" + value + ")");
        }

        @Override
        public void nullField(String fieldName) {
            events.add("null(" + fieldName + ")");
        }

        @Override
        public void startArray(String fieldName) {
            events.add("startArray(" + fieldName + ")");
        }

        @Override
        public void endArray() {
            events.add("endArray()");
        }

        @Override
        public void arrayElemString(byte[] buf, int off, int len) {
            events.add("arrayElemString(" + new String(buf, off, len, StandardCharsets.UTF_8) + ")");
        }

        @Override
        public void arrayElemLong(long value, boolean fitsInt) {
            events.add("arrayElemLong(" + value + ",fitsInt=" + fitsInt + ")");
        }

        @Override
        public void arrayElemBigInteger(BigInteger value, byte[] srcBuf, int srcOff, int srcLen) {
            events.add("arrayElemBigInteger(" + value + ")");
        }

        @Override
        public void arrayElemDouble(double value, boolean fitsFloat) {
            events.add("arrayElemDouble(" + value + ",fitsFloat=" + fitsFloat + ")");
        }

        @Override
        public void arrayElemBoolean(boolean value) {
            events.add("arrayElemBoolean(" + value + ")");
        }

        @Override
        public void arrayElemNull() {
            events.add("arrayElemNull()");
        }

        @Override
        public void arrayElemStartObject() {
            events.add("arrayElemStartObject()");
        }

        @Override
        public void arrayElemEndObject() {
            events.add("arrayElemEndObject()");
        }

        @Override
        public void arrayElemStartArray() {
            events.add("arrayElemStartArray()");
        }

        @Override
        public void arrayElemEndArray() {
            events.add("arrayElemEndArray()");
        }
    }

    // ---- Walk helpers ----

    /**
     * Parses a single JSON object document with {@link SimdJsonDirectWalker} and returns
     * the recorded events.
     */
    static List<String> walkJson(String json) {
        return walkJson(json, false);
    }

    /**
     * Parses a single JSON object document with {@link SimdJsonDirectWalker} and returns
     * the recorded events. When {@code normalizeEmptyObject} is true, empty objects are
     * emitted as {@code startObject/endObject} pairs (for Jackson comparison).
     */
    static List<String> walkJson(String json, boolean normalizeEmptyObject) {
        byte[] buffer = json.getBytes(StandardCharsets.UTF_8);
        int len = buffer.length;

        SimdJsonBatchParser parser = newParser(len);
        parser.stage1(buffer, len);
        parser.prepareDocumentWindow(0, len);

        FrozenFieldNameTable parent = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = parent.makeChild();
        SimdJsonDirectWalker walker = new SimdJsonDirectWalker(child);

        RecordingHandler handler = new RecordingHandler(normalizeEmptyObject);
        walker.walkDocument(buffer, len, parser.bitIndexes(), handler);
        return handler.events;
    }

    // ---- Parser factory ----

    static SimdJsonBatchParser newParser(int capacity) {
        return new SimdJsonBatchParser(capacity, SimdJsonTestSupport::scalarStage1);
    }

    // ---- Buffer helpers ----

    /** Converts a string to a UTF-8 byte array. */
    public static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Creates a byte array with the content placed at {@code offset}. */
    public static byte[] toBytesAtOffset(String content, int offset) {
        byte[] raw = content.getBytes(StandardCharsets.UTF_8);
        byte[] buf = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buf, offset, raw.length);
        return buf;
    }

    /** Wraps a string in JSON quotes: {@code "content"}. */
    public static byte[] makeJsonString(String content) {
        return ("\"" + content + "\"").getBytes(StandardCharsets.UTF_8);
    }

    // ---- Batch buffer helpers ----

    /** Concatenates JSON docs into a single buffer. */
    static byte[] buildBatchBuffer(String... jsonDocs) {
        List<byte[]> docBytes = new ArrayList<>();
        int total = 0;
        for (String doc : jsonDocs) {
            byte[] b = doc.getBytes(StandardCharsets.UTF_8);
            docBytes.add(b);
            total += b.length;
        }
        byte[] buffer = new byte[total];
        int pos = 0;
        for (byte[] b : docBytes) {
            System.arraycopy(b, 0, buffer, pos, b.length);
            pos += b.length;
        }
        return buffer;
    }

    /** Returns the byte offset of each doc within a concatenated batch. */
    static int[] computeOffsets(String... jsonDocs) {
        int[] offsets = new int[jsonDocs.length];
        int pos = 0;
        for (int i = 0; i < jsonDocs.length; i++) {
            offsets[i] = pos;
            pos += jsonDocs[i].getBytes(StandardCharsets.UTF_8).length;
        }
        return offsets;
    }

    /** Returns the byte length of each doc. */
    static int[] computeLengths(String... jsonDocs) {
        int[] lengths = new int[jsonDocs.length];
        for (int i = 0; i < jsonDocs.length; i++) {
            lengths[i] = jsonDocs[i].getBytes(StandardCharsets.UTF_8).length;
        }
        return lengths;
    }

    /** Sums an array of lengths. */
    static int totalLen(int[] lengths) {
        int total = 0;
        for (int len : lengths) {
            total += len;
        }
        return total;
    }

    // ---- Random JSON generation ----

    private static final java.util.Random SHARED_RANDOM = new java.util.Random();

    /** Generates a random JSON object with up to {@code maxFields} fields and {@code maxDepth} nesting. */
    public static String generateRandomJsonObject(int maxFields, int maxDepth) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        int fieldCount = 1 + SHARED_RANDOM.nextInt(maxFields);
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append("field").append(i).append('"').append(':');
            appendRandomValue(sb, maxFields, maxDepth, 0);
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendRandomValue(StringBuilder sb, int maxFields, int maxDepth, int depth) {
        int type = SHARED_RANDOM.nextInt(depth < maxDepth ? 7 : 5);
        switch (type) {
            case 0 -> sb.append('"').append("str").append(SHARED_RANDOM.nextInt(1000)).append('"');
            case 1 -> sb.append(SHARED_RANDOM.nextInt(200000) - 100000);
            case 2 -> sb.append(SHARED_RANDOM.nextDouble() * 1000);
            case 3 -> sb.append(SHARED_RANDOM.nextBoolean());
            case 4 -> sb.append("null");
            case 5 -> {
                sb.append('{');
                int fc = 1 + SHARED_RANDOM.nextInt(Math.max(1, maxFields - 1));
                for (int i = 0; i < fc; i++) {
                    if (i > 0) sb.append(',');
                    sb.append('"').append("f").append(depth).append('_').append(i).append('"').append(':');
                    appendRandomValue(sb, maxFields, maxDepth, depth + 1);
                }
                sb.append('}');
            }
            case 6 -> {
                sb.append('[');
                int ac = SHARED_RANDOM.nextInt(5);
                for (int i = 0; i < ac; i++) {
                    if (i > 0) sb.append(',');
                    appendRandomValue(sb, maxFields, maxDepth, depth + 1);
                }
                sb.append(']');
            }
        }
    }

    /** Drains all structural characters from a {@link BitIndexes} into a list. */
    static List<Character> drainStructurals(byte[] buffer, BitIndexes bi) {
        bi.setReadWindow(0, bi.writeCount());
        List<Character> chars = new ArrayList<>();
        while (!bi.isEnd()) {
            int idx = bi.getAndAdvance();
            chars.add((char) buffer[idx]);
        }
        return chars;
    }
}
