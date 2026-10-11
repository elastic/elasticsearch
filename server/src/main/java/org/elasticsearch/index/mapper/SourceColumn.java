/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * One layer of a field's contribution to the {@code columnar_stored} {@code _source} blob on the columnar batch path, built directly
 * from the values a {@link FieldMapper} already mapped. It is the direct-write counterpart of a single
 * {@link CompositeSyntheticFieldLoader.Layer}: a field mapper registers one {@code SourceColumn} per synthetic-source layer it owns
 * (the doc-values layer first, then any fallback layers in loader order), and {@link CompositeSourceColumn} combines them with the
 * same scalar-or-array rule the loader applies, so the blob is byte-for-byte identical to the loader path without paying the
 * per-document loader cost (reset / advanceToDoc / prepare / repopulate).
 *
 * <p>A {@code SourceColumn} answers two questions per document — how many values the field contributes, and how to write them — and is
 * forward-only: {@link #valueCount(int)} and {@link #writeValues(int, XContentBuilder)} are called once per document in strictly
 * increasing document order, so an implementation may keep a cursor and never has to search. It writes only bare values, never the
 * field name or an enclosing array; that framing belongs to {@link CompositeSourceColumn}, exactly as a loader layer writes bare
 * values and {@link CompositeSyntheticFieldLoader#write} owns the field name and array.</p>
 */
interface SourceColumn {

    /**
     * The number of values this layer contributes for {@code doc}. Summed across a field's layers by
     * {@link CompositeSourceColumn} to decide, per the loader's rule, whether the field is written as a scalar (total of one) or an
     * array (anything else).
     */
    int valueCount(int doc);

    /**
     * Writes this layer's bare values for {@code doc} into {@code builder}, in the order the loader would, without a field name or an
     * enclosing array. The number of values written must equal {@link #valueCount(int)} for the same document.
     */
    void writeValues(int doc, XContentBuilder builder) throws IOException;
}
