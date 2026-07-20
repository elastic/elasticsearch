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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * A single ESCF column held in its <b>native</b> in-memory representation — the metadata factors are
 * live arrays/bitsets rather than pre-serialized bytes
 * <ul>
 *   <li>{@code absent} — validity bitset (bit set = absent); {@code null} when every document is present.</li>
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
 * </ul>
 */
record EscfColumnData(
    byte kind,
    int docCount,
    FixedBitSet absent,
    FixedBitSet values,
    BytesRef typeVector,
    int[] offsets,
    BytesReference data,
    EscfColumnData child
) implements Releasable {

    /**
     * Releases the recycler-backed buffers this column owns: its own {@code data} payload (when it is
     * {@link Releasable}) and, for ARRAY, its nested {@code child} column's buffers. Only columns built
     * natively by {@link EscfColumnBuilder} own releasable buffers; columns reconstructed from a
     * serialized batch hold non-releasable slices, for which this is a no-op.
     */
    @Override
    public void close() {
        Releasables.close(data instanceof Releasable releasable ? releasable : null, child);
    }

    /** LONG or DOUBLE: a dense 8-byte-per-document value payload; no offsets or type vector. */
    static EscfColumnData ofFixed64(byte kind, int docCount, FixedBitSet absent, BytesReference data) {
        return new EscfColumnData(kind, docCount, absent, null, null, null, data, null);
    }

    /** BOOL: the value bitset directly; no byte payload. */
    static EscfColumnData ofBool(int docCount, FixedBitSet absent, FixedBitSet values) {
        return new EscfColumnData(EscfColumnKind.BOOL, docCount, absent, values, null, null, null, null);
    }

    /** STRING or BINARY: an offset vector plus a dense byte payload. */
    static EscfColumnData ofVarWidth(byte kind, int docCount, FixedBitSet absent, int[] offsets, BytesReference data) {
        return new EscfColumnData(kind, docCount, absent, null, null, offsets, data, null);
    }

    /** ARRAY: per-row element-range offsets over a native primitive {@code child} sub-column. */
    static EscfColumnData ofArray(int docCount, FixedBitSet absent, int[] offsets, EscfColumnData child) {
        return new EscfColumnData(EscfColumnKind.ARRAY, docCount, absent, null, null, offsets, null, child);
    }

    /** UNION: a per-document type vector, an offset vector, and a dense value payload. */
    static EscfColumnData ofUnion(int docCount, FixedBitSet absent, BytesRef typeVector, int[] offsets, BytesReference data) {
        return new EscfColumnData(EscfColumnKind.UNION, docCount, absent, null, typeVector, offsets, data, null);
    }
}
