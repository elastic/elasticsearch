/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Where a read schema came from — the axis that {@code dynamic} actually selects, and the one input several read-time
 * decisions turn on. Carried alongside a {@code readSchema} so the data node knows what kind of schema it holds without
 * being told the reading mode: {@code dynamic} is not a thing the reader should reason about, provenance is.
 * <p>
 * A schema is a <em>description</em> of a file or a <em>claim</em> about it, and that difference — not strict-vs-dynamic
 * — is what drives:
 * <ul>
 *   <li><b>binding</b> — an {@link #INFERRED} schema was read from the file, so its <em>i</em>-th column already is the
 *       file's <em>i</em>-th physical field: bind by position. A {@link #DECLARED} schema asserts names, not positions,
 *       so it must bind by name against the file's own physical-name space (header line, columnar footer, JSON keys, or
 *       the self-encoded {@code col<N>} names of a headerless text file); a name no physical column supplies is an
 *       error, not a silent positional fallback.</li>
 *   <li><b>existence</b> — a {@link #DECLARED} column absent from the file is a mismatch between the claim and reality
 *       and is surfaced (the declaration still wins — the column reads null — but the divergence is reported, not
 *       swallowed). An {@link #INFERRED} column cannot be absent: it was read from the file.</li>
 *   <li><b>coercion</b> — a {@link #DECLARED} type licenses a lossy read-time narrowing toward it; an {@link #INFERRED}
 *       target may only widen. (Tracked per-column by {@code declaredTypeColumns}; provenance is the schema-level shape
 *       of the same distinction.)</li>
 * </ul>
 * Under {@code dynamic:true} the schema is {@code INFERRED} (read from the file); under {@code dynamic:false} it is a
 * {@code DECLARED} claim. That is the whole of what the mode controls — every downstream behavior reads this, never the
 * mode.
 */
public enum SchemaProvenance {
    INFERRED,
    DECLARED
}
