/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.escf.LuceneBinaryColumn;
import org.elasticsearch.escf.LuceneLongColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Applies {@code ignore_malformed: true} to a columnar batch the way {@link FallbackPostMapper#captureMalformed} does per document on
 * the row path, so that mappers gaining batch support do not each re-implement its encoding and index-version rules.
 *
 * <p>For every malformed value it reports the field in {@code _ignored} (once per document) and, when the row path would store the
 * value, buffers it for the {@code ._ignore_malformed} or {@code ._on_failure} sidecar column. {@link #emit} attaches those columns.
 * Where the index layout cannot represent a malformed value on the batch path, {@code record} throws
 * {@link UnsupportedOperationException}, which sends the batch back to the row path; this is checked per value, so a batch with no
 * malformed values never falls back because of it.
 *
 * <p>Fields that record offsets are not supported: their malformed values go to {@code _ignored_source} on the row path.
 * Obtain an instance from {@link BatchMappingContext#malformedValues}.
 */
public final class BatchMalformedValues implements Releasable {
    private final BatchMappingContext ctx;
    private final String fieldPath;
    /** Whether a malformed value can be handled in the batch. */
    private final boolean handled;
    /** Whether malformed values are written to a column, or only reported in {@code _ignored}. */
    private final boolean store;
    private final boolean onFailureColumn;
    private EscfColumnBuilder values;
    private EscfColumnBuilder counts;
    /** The current document's malformed values. */
    private List<BytesRef> pending;
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private int pendingDoc = -1;

    BatchMalformedValues(BatchMappingContext ctx, String fieldPath, boolean ignoreMalformed, boolean storeMalformed) {
        this.ctx = ctx;
        this.fieldPath = fieldPath;
        final IndexSettings indexSettings = ctx.indexSettings();
        this.handled = supported(indexSettings, ignoreMalformed, storeMalformed);
        this.onFailureColumn = FallbackPostMapper.malformedUsesOnFailureColumn(indexSettings);
        // Same gate as the row path.
        this.store = storeMalformed && (onFailureColumn == false || FallbackPostMapper.storesOnFailureValues(ctx.mappingLookup()));
    }

    /**
     * Whether the batch path can apply {@code ignore_malformed} for this index: it must be on, and when the row path would store the
     * value, the index version must have a sidecar column layout the batch path can write.
     *
     * @param ignoreMalformed the field's {@code ignore_malformed} setting
     * @param storeMalformed  whether the row path stores malformed values so that synthetic source can restore them
     */
    public static boolean supported(IndexSettings indexSettings, boolean ignoreMalformed, boolean storeMalformed) {
        if (ignoreMalformed == false) {
            return false;
        }
        if (storeMalformed == false) {
            return true;
        }
        final IndexVersion version = indexSettings.getIndexVersionCreated();
        return MultiValuedBinaryDocValuesField.useSeparateCount(version)
            && (FallbackPostMapper.malformedUsesOnFailureColumn(indexSettings)
                || version.onOrAfter(IndexVersions.STORE_IGNORED_MALFORMED_IN_BINARY_DOC_VALUES));
    }

    /** Whether {@code record} can handle a malformed value, rather than throwing. */
    public boolean handled() {
        return handled;
    }

    /**
     * Records a malformed value, already encoded as {@link XContentDataHelper#encodeToken} encodes it, found in document {@code doc}.
     * Values must be recorded in document order.
     *
     * @throws UnsupportedOperationException if the batch cannot handle malformed values, so it falls back to the row path
     */
    public void record(int doc, BytesRef encoded) {
        checkHandled(null);
        recordEncoded(doc, encoded);
    }

    /**
     * Records the malformed scalar value under {@code cursor}, found in document {@code doc}, encoding it as the row path does.
     * Values must be recorded in document order.
     *
     * @throws UnsupportedOperationException if the batch cannot handle the value, so it falls back to the row path
     */
    public void record(int doc, EscfColumnTransforms.TypedUtf8Cursor cursor) {
        checkHandled(cursor);
        // The value is only encoded when it is going to be stored.
        recordEncoded(doc, store ? encode(cursor) : null);
    }

    private void checkHandled(EscfColumnTransforms.TypedUtf8Cursor cursor) {
        if (handled == false) {
            throw new UnsupportedOperationException(
                "mapColumnBatch: malformed value"
                    + (cursor != null && cursor.value() != null ? " [" + cursor.value().utf8ToString() + "]" : "")
                    + " in field ["
                    + fieldPath
                    + "] cannot be handled in a batch"
            );
        }
    }

    private void recordEncoded(int doc, BytesRef encoded) {
        if (doc != pendingDoc) {
            flush();
            pendingDoc = doc;
            // Report the field in _ignored once per document.
            ctx.addIgnoredFieldColumnar(doc, fieldPath);
        }
        if (store) {
            if (pending == null) {
                pending = new ArrayList<>();
            }
            pending.add(encoded);
        }
    }

    /** Encodes the value as the row path stores it: numbers and booleans keep the type they were written as. */
    private BytesRef encode(EscfColumnTransforms.TypedUtf8Cursor cursor) {
        final byte type = cursor.valueType();
        return switch (type) {
            case SourceValueType.STRING -> XContentDataHelper.encodeString(cursor.value());
            case SourceValueType.INT, SourceValueType.LONG -> XContentDataHelper.encodeIntegral(cursor.longValue());
            // A FLOAT is a lossless compaction of a double, so store it as the double.
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> XContentDataHelper.encodeDouble(cursor.doubleValue());
            case SourceValueType.TRUE -> XContentDataHelper.encodeBoolean(true);
            case SourceValueType.FALSE -> XContentDataHelper.encodeBoolean(false);
            // NULL is handled by the callers; the other types cannot be converted to a string.
            case SourceValueType.NULL, SourceValueType.BINARY, SourceValueType.KEY_VALUE, SourceValueType.FIXED_ARRAY,
                SourceValueType.UNION_ARRAY -> throw new UnsupportedOperationException(
                    "mapColumnBatch: malformed value of unsupported type [" + SourceValueType.name(type) + "] in field [" + fieldPath + "]"
                );
            default -> throw new UnsupportedOperationException(
                "mapColumnBatch: malformed value of unknown type [" + type + "] in field [" + fieldPath + "]"
            );
        };
    }

    private EscfColumnBuilder newBuilder(byte kind) {
        final EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.MERGE, ctx.recycler());
        builder.lockScalar(kind);
        return builder;
    }

    private void flush() {
        if (pending == null || pending.isEmpty()) {
            return;
        }
        if (values == null) {
            values = newBuilder(EscfColumnKind.STRING);
            counts = newBuilder(EscfColumnKind.LONG);
        }
        // ._ignore_malformed sorts a document's values; ._on_failure keeps their order.
        if (onFailureColumn == false) {
            pending.sort(Comparator.naturalOrder());
        }
        if (pending.size() == 1) {
            values.setString(pendingDoc, pending.get(0));
        } else {
            // SeparateCount layout: [vint length][value]... for several values, the raw value for one.
            scratch.clear();
            for (BytesRef value : pending) {
                appendVInt(scratch, value.length);
                scratch.append(value.bytes, value.offset, value.length);
            }
            values.setString(pendingDoc, scratch.get());
        }
        counts.setLong(pendingDoc, pending.size());
        pending.clear();
    }

    private static void appendVInt(BytesRefBuilder out, int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            out.append((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.append((byte) v);
    }

    /** Attaches the columns holding the stored malformed values, if any. Call once, after the last {@code record}. */
    public void emit() {
        flush();
        if (values != null) {
            final String columnName = onFailureColumn ? OnFailureStoredValues.name(fieldPath) : IgnoreMalformedStoredValues.name(fieldPath);
            final int docCount = ctx.docCount();
            final EscfColumnData valuesData = values.finish(docCount);
            ctx.addColumn(LuceneBinaryColumn.of(valuesData, columnName, CustomDocValuesField.TYPE), valuesData);
            final EscfColumnData countData = counts.finish(docCount);
            ctx.addColumn(LuceneLongColumn.counts(countData, columnName), countData);
        }
    }

    @Override
    public void close() {
        Releasables.close(values, counts);
    }
}
