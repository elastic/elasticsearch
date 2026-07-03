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

/**
 * A single ESCF column held in its <b>native</b> in-memory representation — the metadata factors are
 * live arrays/bitsets rather than pre-serialized bytes, so the in-memory build→index path never round
 * trips through the wire encoding. Bytes are produced only when {@link EscfBatch#data()} serializes.
 * <ul>
 *   <li>{@code absent} — validity bitset (bit set = absent); {@code null} when every document is present.</li>
 *   <li>{@code values} — the BOOL value bitset (bit set = {@code true}); {@code null} for every other kind.</li>
 *   <li>{@code typeVector} — one {@link org.elasticsearch.sourcebatch.SourceValueType} byte per document; {@code null}
 *       for kinds whose per-document type is implied by {@link #kind} (everything except UNION).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} entries; {@code null} for fixed-width kinds (LONG, DOUBLE) and BOOL.
 *       For STRING/BINARY/UNION these are byte offsets into {@code data}; for ARRAY they are per-row element-range
 *       offsets into the child sub-column.</li>
 *   <li>{@code data} — the recycler-backed value payload ({@code null} for BOOL, whose values live in
 *       {@code values}). For ARRAY it is {@code child_kind(1) | child_values}.</li>
 * </ul>
 */
record ElasticsearchColumnData(
    byte kind,
    int docCount,
    FixedBitSet absent,
    FixedBitSet values,
    byte[] typeVector,
    int[] offsets,
    BytesReference data
) {}
