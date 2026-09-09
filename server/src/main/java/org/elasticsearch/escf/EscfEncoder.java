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
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdjson.JsonDocumentParser;
import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.simdjson.SimdJsonParserPool;
import org.elasticsearch.simdjson.SimdJsonSupport;
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
 *   <li>JSON, no larger than {@link JsonDocumentParser#maxDocumentBytes()}, and
 *       {@link #isSimdEnabled()}: this thread's {@link JsonDocumentParser} (native SIMD stage 1 +
 *       fused stage 2/walk). Falls back to Jackson on any failure.</li>
 *   <li>Otherwise: Jackson stream parser.</li>
 * </ol>
 */
public final class EscfEncoder implements SourceBatchEncoder {

    private static final Logger logger = LogManager.getLogger(EscfEncoder.class);

    /**
     * Feature flag for the simdjson-backed ESCF JSON encode path. In snapshot builds it defaults
     * to enabled; in release builds it defaults to disabled and can be turned on with
     * {@code -Des.simdjson_escf_feature_flag_enabled=true}.
     */
    static final FeatureFlag SIMDJSON_ESCF_FEATURE_FLAG = new FeatureFlag("simdjson_escf");

    private final EscfBatchBuilder backend;

    /**
     * This thread's shared parser, or {@code null} when the SIMD path is unavailable or switched
     * off. Resolved once here rather than per document; safe because an encoder is used only on the
     * thread that created it (a bulk's coordinating pass is synchronous and single-threaded).
     */
    @Nullable
    private final JsonDocumentParser docParser;

    /**
     * Staging area for sources that are not array-backed, allocated on first need. Owned by this
     * encoder rather than shared per-thread, so its contents cannot be clobbered by another
     * encoder running on the same thread.
     */
    private byte[] scratch;

    public EscfEncoder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE, true);
    }

    public EscfEncoder(Recycler<BytesRef> recycler) {
        this(recycler, true);
    }

    /**
     * @param allowSimd pass {@code false} to force the Jackson path; used by tests and benchmarks
     *                  to obtain a baseline for differential comparison
     */
    public EscfEncoder(Recycler<BytesRef> recycler, boolean allowSimd) {
        this.backend = new EscfBatchBuilder(recycler);
        this.docParser = allowSimd && isSimdEnabled() ? SimdJsonParserPool.getDefault().forCurrentThread() : null;
    }

    /**
     * Whether the simdjson ESCF encode path may be used at all: the native library is loaded, the
     * vector API is available, and {@link #SIMDJSON_ESCF_FEATURE_FLAG} is enabled.
     */
    static boolean isSimdEnabled() {
        return SimdJsonSupport.isSupported() && SIMDJSON_ESCF_FEATURE_FLAG.isEnabled();
    }

    public void parseToScratch(BytesReference source, XContentType xContentType) throws IOException {
        parseToScratch(source, xContentType, LeafSink.NO_OP);
    }

    @Override
    public void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException {
        if (tryDirectWalkSingle(source, xContentType, sink)) {
            return;
        }
        EscfRowBuffer row = backend.beginRow();
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            if (xContentType == XContentType.JSON) {
                parser.allowDuplicateKeys(true);
            }
            parser.nextToken(); // START_OBJECT
            flattenObject(row, parser, parser.nextToken(), sink);
        }
        row.finishRow();
    }

    /**
     * Attempts to parse a single document using the direct walker (SIMD stage 1 + fused walk).
     * Returns true if successful, false if the document is ineligible or parsing failed
     * (in which case the caller falls back to Jackson).
     */
    private boolean tryDirectWalkSingle(BytesReference source, XContentType xContentType, LeafSink sink) {
        if (docParser == null || xContentType.canonical() != XContentType.JSON || source.length() > docParser.maxDocumentBytes()) {
            return false;
        }

        byte[] buf;
        int offset;
        try {
            if (source.hasArray()) {
                buf = source.array();
                offset = source.arrayOffset();
            } else {
                buf = copyToScratch(source);
                offset = 0;
            }
        } catch (IOException e) {
            return false;
        }
        int len = source.length();

        try {
            EscfRowBuffer row = backend.beginRow();
            boolean rawTextMode = sink != LeafSink.NO_OP && sink.passRawText();
            EscfDocumentHandler handler = new EscfDocumentHandler(row, backend, sink, rawTextMode);
            docParser.parseDocument(buf, offset, len, handler);
            row.finishRow();
            return true;
        } catch (JsonParsingException e) {
            // The handler may have written part of the row already; the next beginRow() discards it.
            logger.debug(() -> "Direct walk failed, falling back to Jackson: " + e.getMessage());
            return false;
        } catch (RuntimeException e) {
            logger.warn("Unexpected direct walk failure, falling back to Jackson", e);
            return false;
        }
    }

    /**
     * Copies a non-array-backed source into this encoder's scratch buffer for SIMD parsing.
     * Array-backed sources (including bulk slices with a non-zero {@code arrayOffset()}) are used
     * in place by the caller instead.
     *
     * <p>The returned scratch is only valid until the next call.
     */
    private byte[] copyToScratch(BytesReference source) throws IOException {
        int len = source.length();
        if (scratch == null) {
            scratch = new byte[docParser.maxDocumentBytes()];
        }
        int pos = 0;
        BytesRefIterator it = source.iterator();
        for (BytesRef page = it.next(); page != null; page = it.next()) {
            System.arraycopy(page.bytes, page.offset, scratch, pos, page.length);
            pos += page.length;
        }
        assert pos == len : pos + " != " + len;
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

    /**
     * Releases the backend and publishes the field names this encoder learned, so other threads can
     * reuse them. The parser itself is owned by the pool and outlives this encoder.
     */
    @Override
    public void close() {
        try {
            if (docParser != null) {
                docParser.publishFieldNames();
            }
        } finally {
            backend.close();
        }
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
