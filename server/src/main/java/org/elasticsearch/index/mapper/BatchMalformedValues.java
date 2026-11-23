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
import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Applies {@code ignore_malformed: true} to a batch as the row path does: reports the field in {@code _ignored} and stores the value in
 * a sidecar column. Where it cannot, {@code record} throws {@link UnsupportedOperationException}, sending the batch to the row path.
 */
public final class BatchMalformedValues implements Releasable {
    private final BatchMappingContext ctx;
    /** Names the sidecar columns, as on the row path. */
    private final String fieldPath;
    private final BytesRef ignoredName;
    private final boolean ignoreMalformed;
    /** Whether the index layout can hold the values the row path would store. */
    private final boolean layoutSupported;
    /** Whether malformed values are written to a column, or only reported in {@code _ignored}. */
    private final boolean store;
    private final boolean onFailureColumn;
    private EscfColumnBuilder values;
    private EscfColumnBuilder counts;
    /** The current document's malformed values. */
    private List<BytesRef> pending;
    private BytesRefBuilder scratch;
    private int pendingDoc = -1;
    private boolean emitted;

    BatchMalformedValues(BatchMappingContext ctx, FieldMapper mapper, boolean storeMalformed) {
        // On the row path, fields with offsets also record malformed values with the offsets, which the batch path does not do.
        assert mapper.getOffsetFieldName() == null : "field [" + mapper.fullPath() + "] records offsets, so it must use the row path";
        this.ctx = ctx;
        this.fieldPath = mapper.fullPath();
        // The row path reports the field type's name in _ignored, but names the sidecar columns after the full path.
        this.ignoredName = new BytesRef(mapper.fieldType().name());
        this.ignoreMalformed = mapper.ignoreMalformed();
        final IndexSettings indexSettings = ctx.indexSettings();
        this.layoutSupported = layoutSupported(indexSettings.getIndexVersionCreated(), storeMalformed);
        this.onFailureColumn = FallbackPostMapper.malformedUsesOnFailureColumn(indexSettings);
        // As on the row path, ._on_failure values are only written when the source is reconstructed from the index.
        this.store = storeMalformed && (onFailureColumn == false || FallbackPostMapper.storesOnFailureValues(ctx.mappingLookup()));
    }

    /**
     * Storing needs the {@code SeparateCount} layout; the {@code ._ignore_malformed} version gate is older and the {@code ._on_failure}
     * one newer, so neither adds a constraint.
     */
    static boolean layoutSupported(IndexVersion version, boolean storeMalformed) {
        return storeMalformed == false || MultiValuedBinaryDocValuesField.useSeparateCount(version);
    }

    /** Whether {@code record} can handle a malformed value rather than throw; a mapper may check it to fall back early. */
    public boolean handled() {
        return ignoreMalformed && layoutSupported;
    }

    /**
     * Records a malformed value, encoded as {@link XContentDataHelper#encodeToken} does, for {@code doc}. The bytes are copied, and
     * documents must be recorded in ascending order.
     */
    public void record(int doc, BytesRef encoded) {
        checkHandled(null);
        recordEncoded(doc, store ? BytesRef.deepCopyOf(encoded) : null);
    }

    /**
     * Records the malformed scalar under {@code cursor} for {@code doc}, encoded as the row path stores it. Documents must be recorded
     * in ascending order.
     */
    public void record(int doc, EscfColumnTransforms.TypedUtf8Cursor cursor) {
        checkHandled(cursor);
        recordEncoded(doc, store ? encode(cursor) : null);
    }

    private void checkHandled(EscfColumnTransforms.TypedUtf8Cursor cursor) {
        if (handled()) {
            return;
        }
        final String reason = ignoreMalformed
            ? "the index version cannot store malformed values in a batch"
            : "ignore_malformed is false, and rejecting documents is not supported in a batch";
        final String value = cursor != null && cursor.value() != null ? " [" + cursor.value().utf8ToString() + "]" : "";
        throw new UnsupportedOperationException("mapColumnBatch: malformed value" + value + " in field [" + fieldPath + "]: " + reason);
    }

    private void recordEncoded(int doc, BytesRef encoded) {
        assert emitted == false : "record after emit";
        assert doc >= pendingDoc : "documents must be recorded in order, got [" + doc + "] after [" + pendingDoc + "]";
        if (doc != pendingDoc) {
            flush();
            pendingDoc = doc;
            ctx.addIgnoredFieldColumnar(doc, ignoredName);
        }
        if (store) {
            if (pending == null) {
                pending = new ArrayList<>();
            }
            pending.add(encoded);
        }
    }

    /** Numbers and booleans keep the type they were written as, as on the row path. */
    private BytesRef encode(EscfColumnTransforms.TypedUtf8Cursor cursor) {
        final byte type = cursor.valueType();
        return switch (type) {
            case SourceValueType.STRING -> XContentDataHelper.encodeString(cursor.value());
            case SourceValueType.INT, SourceValueType.LONG -> XContentDataHelper.encodeIntegral(cursor.longValue());
            // A FLOAT is a lossless compaction of a double, so store it as the double.
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> XContentDataHelper.encodeDouble(cursor.doubleValue());
            case SourceValueType.TRUE -> XContentDataHelper.encodeBoolean(true);
            case SourceValueType.FALSE -> XContentDataHelper.encodeBoolean(false);
            // Callers handle NULL; the other types are not supported, so the batch falls back to the row path.
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
        if (onFailureColumn == false && pending.size() > 1) {
            pending.sort(Comparator.naturalOrder());
        }
        if (pending.size() == 1) {
            values.setString(pendingDoc, pending.get(0));
        } else {
            // SeparateCount layout: [vint length][value]... for several values, the raw value for one.
            if (scratch == null) {
                scratch = new BytesRefBuilder();
            }
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

    /** Attaches the sidecar columns holding the stored values, if any; call once, after the last {@code record}. */
    public void emit() {
        assert emitted == false : "emit called twice";
        emitted = true;
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
