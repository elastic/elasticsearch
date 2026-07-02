/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.common.bytes.BytesReference;

/**
 * The serialized form of a single ESCF column, held as up to four independent fields rather than one
 * pre-concatenated blob:
 * <ul>
 *   <li>{@code absentBitset} — LE-long bitset, bit set = absent; {@code null} when no document is absent.</li>
 *   <li>{@code typeVector} — one {@link org.elasticsearch.sourcebatch.SourceValueType} byte per document; {@code null}
 *       for kinds whose per-document type is implied by {@link #kind} (everything except UNION).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} little-endian {@code i32} values; {@code null} for
 *       fixed-width kinds (LONG, DOUBLE) and BOOL. For STRING/BINARY/UNION these are byte offsets into
 *       {@code data}; for ARRAY they are per-row element-range offsets into the child sub-column.</li>
 *   <li>{@code data} — the value payload; never {@code null}, may be empty. For ARRAY it is
 *       {@code child_kind(1) | child_values}.</li>
 * </ul>
 *
 * <p>This holder performs no concatenation: an in-memory {@link EscfBatch} reads directly from these
 * fields, and they are joined into a single {@link BytesReference} only on {@link EscfBatch#data()}.
 */
record ElasticsearchColumnData(
    byte kind,
    int docCount,
    BytesReference absentBitset,
    BytesReference typeVector,
    BytesReference offsets,
    BytesReference data
) {}
