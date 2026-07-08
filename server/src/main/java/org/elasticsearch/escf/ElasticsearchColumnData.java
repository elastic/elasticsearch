/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * A single ESCF column held in its <b>native</b> in-memory representation — the metadata factors are
 * live arrays/bitsets rather than pre-serialized bytes
 * <ul>
 *   <li>{@code absent} — validity bitset (bit set = absent); {@code null} when every document is present.</li>
 *   <li>{@code values} — the BOOL value bitset (bit set = {@code true}); {@code null} for every other kind.</li>
 *   <li>{@code typeVector} — one {@link SourceValueType} byte per document; {@code null} for kinds whose
 *       per-document type is implied by {@link #kind} (everything except UNION).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} entries; {@code null} for fixed-width kinds (LONG, DOUBLE) and BOOL.
 *       For STRING/BINARY/UNION these are byte offsets into {@code data}; for ARRAY they are per-row element-range
 *       offsets into {@code child}.</li>
 *   <li>{@code data} — the recycler-backed value payload; {@code null} for BOOL (values live in {@code values})
 *       and for ARRAY (the payload lives in {@code child}).</li>
 *   <li>{@code child} — the dense primitive sub-column for ARRAY (itself a native
 *       {@link ElasticsearchColumnData} of kind LONG, DOUBLE, or STRING); {@code null} for every other kind.
 *       Kept native rather than pre-serialized so the "native in-memory" invariant above also holds for
 *       ARRAY; it is only flattened to {@code child_kind(1) | child_values} bytes at the wire boundary in
 *       {@link EscfBatch}.</li>
 * </ul>
 */
record ElasticsearchColumnData(
    byte kind,
    int docCount,
    FixedBitSet absent,
    FixedBitSet values,
    byte[] typeVector,
    int[] offsets,
    BytesReference data,
    ElasticsearchColumnData child
) {

    /** LONG or DOUBLE: a dense 8-byte-per-document value payload; no offsets or type vector. */
    static ElasticsearchColumnData ofFixed64(byte kind, int docCount, FixedBitSet absent, BytesReference data) {
        return new ElasticsearchColumnData(kind, docCount, absent, null, null, null, data, null);
    }

    /** BOOL: the value bitset directly; no byte payload. */
    static ElasticsearchColumnData ofBool(int docCount, FixedBitSet absent, FixedBitSet values) {
        return new ElasticsearchColumnData(ElasticsearchColumnKind.BOOL, docCount, absent, values, null, null, null, null);
    }

    /** STRING or BINARY: an offset vector plus a dense byte payload. */
    static ElasticsearchColumnData ofVarWidth(byte kind, int docCount, FixedBitSet absent, int[] offsets, BytesReference data) {
        return new ElasticsearchColumnData(kind, docCount, absent, null, null, offsets, data, null);
    }

    /** ARRAY: per-row element-range offsets over a native primitive {@code child} sub-column. */
    static ElasticsearchColumnData ofArray(int docCount, FixedBitSet absent, int[] offsets, ElasticsearchColumnData child) {
        return new ElasticsearchColumnData(ElasticsearchColumnKind.ARRAY, docCount, absent, null, null, offsets, null, child);
    }

    /** UNION: a per-document type vector, an offset vector, and a dense value payload. */
    static ElasticsearchColumnData ofUnion(int docCount, FixedBitSet absent, byte[] typeVector, int[] offsets, BytesReference data) {
        return new ElasticsearchColumnData(ElasticsearchColumnKind.UNION, docCount, absent, null, typeVector, offsets, data, null);
    }
}
