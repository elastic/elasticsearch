/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * Where a read schema came from &mdash; the axis that {@code dynamic} actually selects. Carried alongside a
 * {@code readSchema} so the data node knows what kind of schema it holds without being told the reading mode:
 * {@code dynamic} is not a thing the reader should reason about.
 * <p>
 * A schema is a <em>description</em> of a file or a <em>claim</em> about it. That difference decides how a column
 * BINDS, and nothing else:
 * <ul>
 *   <li><b>binding</b> &mdash; an {@link #INFERRED} schema was read from the file, so its <em>i</em>-th column
 *       already is the file's <em>i</em>-th physical field: bind by position. A {@link #DECLARED} schema asserts
 *       names, not positions, so it must bind by name against the file's own physical-name space (header line, or
 *       the self-encoded {@code col<N>} names of a headerless text file). It is never a silent positional
 *       fallback; a declared name the file does not supply is the absence case below.</li>
 *   <li><b>existence</b> &mdash; a {@link #DECLARED} column the file does not supply reads null, and the mismatch
 *       is surfaced by one deduplicated warning so a typo'd name or a dropped upstream column is not invisible. It
 *       is not an error. An {@link #INFERRED} column cannot be absent: it was read from the file.</li>
 * </ul>
 * Only the text readers need either; Parquet, ORC and NDJSON bind by footer name or object key under both values.
 * It is also a component of the read-config cache key, because the two values bind the same columns differently
 * and so can produce different blocks from one file.
 * <p>
 * <b>Nothing about reading a column's VALUES may consult this.</b> Whether a value is converted or the column
 * refused is decided from the two types alone &mdash; what the file holds and what the query reads it as &mdash;
 * because a conversion can fail or not fail for reasons that have nothing to do with who supplied the target type.
 * It once also drove per-value coercion, the error mode's reach, which statistics a column may publish, filter
 * pushdown and the column-count cap every reader applied, which made the same bytes read two different ways depending
 * on whether the dataset happened to carry a mapping. Those reads are gone; see esql-planning#2076 for what replaced
 * them, and do not add another. One cap decision remains outside the readers: the strict rail's coercibility probe
 * raises the column cap at resolution time for a declared dataset.
 * <p>
 * Under {@code dynamic:true} the schema is {@code INFERRED} (read from the file); under {@code dynamic:false} it is
 * a {@code DECLARED} claim. One exception, in the resolver: a {@code dynamic:true} text source whose declaration
 * appended columns the file lacks is upgraded to {@code DECLARED}, so those columns bind by name.
 */
public enum SchemaProvenance {
    INFERRED,
    DECLARED
}
