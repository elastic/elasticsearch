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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * A single ESCF column held in its <b>native</b> in-memory representation — the metadata factors are
 * live arrays/bitsets rather than pre-serialized bytes
 * <ul>
 *   <li>{@code validity} — Validity bitset (bit set = present); {@code null} when every document is present (dense).</li>
 *   <li>{@code values} — the BOOL value bitset (bit set = {@code true}); {@code null} for every other kind.</li>
 *   <li>{@code typeVector} — one {@link SourceValueType} byte per document as a windowed {@link BytesRef};
 *       {@code null} for kinds whose per-document type is implied by {@link #kind} (everything except UNION).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} entries; {@code null} for fixed-width kinds (LONG, DOUBLE) and BOOL.
 *       For STRING/BINARY/UNION these are byte offsets into {@code data}; for ARRAY they are per-row element-range
 *       offsets into {@code child}.</li>
 *   <li>{@code data} — the recycler-backed value payload; {@code null} for BOOL (values live in {@code values})
 *       and for ARRAY (the payload lives in {@code child}).</li>
 *   <li>{@code child} — the dense primitive sub-column for ARRAY (itself a native
 *       {@link EscfColumnData} of kind LONG, DOUBLE, or STRING); {@code null} for every other kind.
 *       Kept native rather than pre-serialized so the "native in-memory" invariant above also holds for
 *       ARRAY; it is only flattened to {@code child_kind(1) | child_values} bytes at the wire boundary in
 *       {@link EscfBatch}.</li>
 *   <li>{@code lexicalForms} — for DOUBLE, an optional native STRING column with one row per document
 *       holding the client's spelling of each value whose
 *       canonical {@code Double.toString} rendering differs from it, zero-length where it does not;
 *       {@code null} for every other kind and for a DOUBLE column in which no value has a stored spelling.</li>
 * </ul>
 */
public record EscfColumnData(
    byte kind,
    int docCount,
    FixedBitSet validity,
    FixedBitSet values,
    BytesRef typeVector,
    int[] offsets,
    BytesReference data,
    EscfColumnData child,
    EscfColumnData lexicalForms
) implements Releasable {

    /**
     * Releases the recycler-backed buffers this column owns: its own {@code data} payload (when it is
     * {@link Releasable}) and, for ARRAY, its nested {@code child} column's buffers. Only columns built
     * natively by {@link EscfColumnBuilder} own releasable buffers; columns reconstructed from a
     * serialized batch hold non-releasable slices, for which this is a no-op.
     */
    @Override
    public void close() {
        Releasables.close(data instanceof Releasable releasable ? releasable : null, child, lexicalForms);
    }

    /** LONG or DOUBLE: a dense 8-byte-per-document value payload; no offsets or type vector. */
    static EscfColumnData ofFixed64(byte kind, int docCount, FixedBitSet validity, BytesReference data) {
        return new EscfColumnData(kind, docCount, validity, null, null, null, data, null, null);
    }

    /**
     * DOUBLE with stored spellings: the dense 8-byte payload plus {@code lexicalForms}, a native STRING column with
     * one row per document holding the spelling of each value whose canonical rendering differs from it (zero-length
     * otherwise); {@code null} when no value has one, which is then the same as {@link #ofFixed64}.
     */
    static EscfColumnData ofDouble(int docCount, FixedBitSet validity, BytesReference data, @Nullable EscfColumnData lexicalForms) {
        assert lexicalForms == null || (lexicalForms.kind() == EscfColumnKind.STRING && lexicalForms.docCount() == docCount)
            : "lexical forms must be a STRING column with one row per value";
        return new EscfColumnData(EscfColumnKind.DOUBLE, docCount, validity, null, null, null, data, null, lexicalForms);
    }

    /** BOOL: the value bitset directly; no byte payload. */
    static EscfColumnData ofBool(int docCount, FixedBitSet validity, FixedBitSet values) {
        return new EscfColumnData(EscfColumnKind.BOOL, docCount, validity, values, null, null, null, null, null);
    }

    /** STRING or BINARY: an offset vector plus a dense byte payload. */
    static EscfColumnData ofVarWidth(byte kind, int docCount, FixedBitSet validity, int[] offsets, BytesReference data) {
        return new EscfColumnData(kind, docCount, validity, null, null, offsets, data, null, null);
    }

    /** ARRAY: per-row element-range offsets over a native primitive {@code child} sub-column. */
    static EscfColumnData ofArray(int docCount, FixedBitSet validity, int[] offsets, EscfColumnData child) {
        return new EscfColumnData(EscfColumnKind.ARRAY, docCount, validity, null, null, offsets, null, child, null);
    }

    /** UNION: a per-document type vector, an offset vector, and a dense value payload. */
    static EscfColumnData ofUnion(int docCount, FixedBitSet validity, BytesRef typeVector, int[] offsets, BytesReference data) {
        return new EscfColumnData(EscfColumnKind.UNION, docCount, validity, null, typeVector, offsets, data, null, null);
    }
}
