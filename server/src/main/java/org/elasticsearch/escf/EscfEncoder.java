/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.SimdJsonBatchParser;
import org.elasticsearch.simdjson.SimdJsonDirectWalker;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceBatchEncoder;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Encodes XContentType documents into {@link EscfBatch}es (Elasticsearch Column Format), accumulating one
 * column per leaf field. Numbers upcast aggressively (JSON int/long → {@code long}, float/double →
 * {@code double}); a type conflict or an explicit null promotes the column to
 * {@link EscfColumnKind#UNION}. Fixed primitive arrays are stored in a columnar list layout;
 * other arrays (heterogeneous, nested, object-bearing) are stored inline on a union column.
 *
 * <p>This class is the x-content frontend: it walks an {@link XContentParser}, populates an
 * {@link EscfRowBuffer}, and delegates all column-building to the shared {@link EscfBatchBuilder}
 * backend. Implements {@link SourceBatchEncoder}. Single-partition convenience:
 * {@link #encode(List, XContentType)}.
 *
 * <p><strong>Parser dispatch:</strong>
 * <ol>
 *   <li>JSON and ≤ {@link SimdJsonPool#MAX_DOC_BYTES}: {@link SimdJsonDirectWalker}
 *       (native SIMD stage 1 + fused stage 2/walk). Falls back to Jackson on any failure.</li>
 *   <li>Otherwise: Jackson stream parser.</li>
 * </ol>
 */
public final class EscfEncoder implements SourceBatchEncoder {

    private static final Logger logger = LogManager.getLogger(EscfEncoder.class);

    private final EscfBatchBuilder backend;
    private final boolean allowSimd;

    public EscfEncoder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    public EscfEncoder(Recycler<BytesRef> recycler) {
        this(recycler, true);
    }

    /**
     * Package-private constructor used by tests to disable the SIMD path and obtain a Jackson
     * baseline for differential comparison.
     */
    EscfEncoder(Recycler<BytesRef> recycler, boolean allowSimd) {
        this.backend = new EscfBatchBuilder(recycler);
        this.allowSimd = allowSimd;
    }

    @Override
    public void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException {
        if (tryDirectWalkSingle(source, xContentType, sink)) {
            return;
        }
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            parser.allowDuplicateKeys(true);
            flattenDocument(parser, sink);
        }
    }

    /**
     * Attempts to parse a single document using the direct walker (SIMD stage 1 + fused walk).
     * Returns true if successful, false if the document is ineligible or parsing failed
     * (in which case the caller falls back to Jackson).
     */
    private boolean tryDirectWalkSingle(BytesReference source, XContentType xContentType, LeafSink sink) {
        if (allowSimd == false
            || SimdJsonPool.AVAILABLE == false
            || xContentType.canonical() != XContentType.JSON
            || source.length() > SimdJsonPool.MAX_DOC_BYTES) {
            return false;
        }

        SimdJsonBatchParser batchParser = SimdJsonPool.batchParser();
        SimdJsonDirectWalker walker = SimdJsonPool.directWalker();

        byte[] buf;
        try {
            buf = simdInput(source);
        } catch (java.io.IOException e) {
            return false;
        }
        int len = source.length();

        try {
            batchParser.stage1(buf, len);
            batchParser.prepareDocumentWindow(0, len);

            EscfRowBuffer row = backend.beginRow();
            boolean rawTextMode = sink != LeafSink.NO_OP && sink.passRawText();
            EscfDocumentHandler handler = new EscfDocumentHandler(row, backend, sink, rawTextMode);
            walker.walkDocument(buf, len, batchParser, handler);
            row.finishRow();
            walker.releaseNames();
            return true;
        } catch (RuntimeException e) {
            // TODO: revert to debug before merging — temporarily WARN to detect fallbacks during Rally benchmarking
            logger.warn("Direct walk single-doc failed, falling back: {}", e.getMessage());
            return false;
        }
    }

    private void flattenDocument(XContentParser parser, LeafSink sink) throws IOException {
        EscfRowBuffer row = backend.beginRow();
        parser.nextToken(); // START_OBJECT
        flattenObject(row, parser, parser.nextToken(), sink);
        row.finishRow();
    }

    /**
     * Returns a byte array containing the source bytes starting at offset 0, suitable for passing
     * to the SIMD structural indexer.
     *
     * <ul>
     *   <li>Zero-copy: if the source is already array-backed with {@code arrayOffset() == 0}.</li>
     *   <li>Single {@code arraycopy}: if array-backed with a non-zero offset (common for bulk slices).</li>
     *   <li>Page-walk copy: if composite / non-contiguous (e.g. {@code CompositeBytesReference}).</li>
     * </ul>
     *
     * <p>The returned array may be the caller's own bytes or the thread-local scratch; it must not
     * be retained past the next call on this thread.
     */
    private static byte[] simdInput(BytesReference source) throws IOException {
        int len = source.length();
        if (source.hasArray() && source.arrayOffset() == 0) {
            return source.array();
        }
        byte[] scratch = SimdJsonPool.scratch();
        if (source.hasArray()) {
            System.arraycopy(source.array(), source.arrayOffset(), scratch, 0, len);
        } else {
            int pos = 0;
            BytesRefIterator it = source.iterator();
            for (BytesRef page = it.next(); page != null; page = it.next()) {
                System.arraycopy(page.bytes, page.offset, scratch, pos, page.length);
                pos += page.length;
            }
            assert pos == len : pos + " != " + len;
        }
        return scratch;
    }

    @Override
    public int commitScratchTo(int partitionKey) {
        return backend.commit(partitionKey);
    }

    @Override
    public EscfBatch buildPartition(int partitionKey) {
        return backend.buildPartition(partitionKey);
    }

    @Override
    public int docCount(int partitionKey) {
        return backend.docCount(partitionKey);
    }

    @Override
    public boolean hasPartition(int partitionKey) {
        return backend.hasPartition(partitionKey);
    }

    @Override
    public String columnPath(int columnIndex) {
        return backend.columnPath(columnIndex);
    }

    @Override
    public void close() {
        backend.close();
    }

    /** Convenience: encodes all {@code sources} into a single-partition batch. */
    public static EscfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private void flattenObject(EscfRowBuffer row, XContentParser parser, XContentParser.Token firstToken, LeafSink sink)
        throws IOException {
        XContentParser.Token token = firstToken;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                // Peek inside the object. An empty object is encoded as its own zero-byte KEY_VALUE leaf so
                // it stays distinguishable from an absent field; non-empty objects flatten recursively.
                XContentParser.Token inner = parser.nextToken();
                if (inner == XContentParser.Token.END_OBJECT) {
                    row.emptyObject(fieldName);
                } else {
                    row.startObject(fieldName);
                    flattenObject(row, parser, inner, sink);
                    row.endObject();
                }
                token = parser.nextToken();
                continue;
            }

            final boolean firePathSink = sink != LeafSink.NO_OP;
            final boolean rawTextMode = firePathSink && sink.passRawText();
            switch (token) {
                case START_ARRAY -> {
                    SourceBatchEncodeHelper.PackedArray arr = SourceBatchEncodeHelper.packArray(parser);
                    int colIdx = row.arrayField(fieldName, arr.arrayType(), arr.packed());
                    if (firePathSink) {
                        sink.onArrayLeaf(colIdx, backend.columnPath(colIdx));
                    }
                }
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    int colIdx = row.stringField(fieldName, str);
                    if (firePathSink) {
                        sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), SourceValueType.STRING, str);
                    }
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            byte type = (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) ? SourceValueType.INT : SourceValueType.LONG;
                            int colIdx = row.longField(fieldName, val);
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onLongPrimitive(colIdx, backend.columnPath(colIdx), type, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            byte type = ((double) fval == val) ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
                            int colIdx = row.doubleField(fieldName, val);
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onDoublePrimitive(colIdx, backend.columnPath(colIdx), type, val);
                            }
                        }
                        default -> {
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            int colIdx = row.stringField(fieldName, str);
                            if (firePathSink) {
                                sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), SourceValueType.STRING, str);
                            }
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    byte type = v ? SourceValueType.TRUE : SourceValueType.FALSE;
                    int colIdx = row.booleanField(fieldName, v);
                    if (rawTextMode) {
                        sink.onTextPrimitive(colIdx, backend.columnPath(colIdx), type, parser.optimizedText().bytes());
                    } else if (firePathSink) {
                        sink.onBooleanPrimitive(colIdx, backend.columnPath(colIdx), v);
                    }
                }
                case VALUE_NULL -> row.nullField(fieldName);
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }
}
