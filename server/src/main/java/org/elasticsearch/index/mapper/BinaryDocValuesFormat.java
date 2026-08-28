/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * How a field lays out its binary doc values, and so which decoder reads them back. One per
 * {@link MultiValuedBinaryDocValuesField} subclass that writes them.
 *
 * <p>The three are not interchangeable, and decoding one as another silently returns wrong values rather than
 * failing — a length prefix reads as a character, a payload's slot count reads as the head of a term. So every
 * reader is told which to expect: the doc-values queries, fielddata, index sorting and the block loaders all
 * take this from the mapping that decided how the values were written.
 *
 * <p>{@link #ordinal()} is persisted in segment info by
 * {@link org.elasticsearch.index.fielddata.plain.MultiValuedBinaryDocValuesSortField}, which Lucene rebuilds at
 * merge time with no mapping in reach. Existing segments hold the {@code 0}/{@code 1} of the boolean this
 * replaced, which is why {@link #SEPARATE_COUNT} and {@link #ARRAY_ORDER_INLINE_NULL} have to keep those
 * positions; append new constants rather than reordering.
 */
public enum BinaryDocValuesFormat {
    /**
     * {@code [len][value]...} with the count in a {@code .counts} companion; a lone value stored raw. Values are
     * sorted and deduplicated, so array order does not survive.
     */
    SEPARATE_COUNT,

    /**
     * {@code [len+1][value]...} with the slot count in a {@code .counts} companion; a lone value stored raw. Slots
     * are in document order and a {@code [vint 0]} slot is a null, which is what the length bias buys.
     */
    ARRAY_ORDER_INLINE_NULL,

    /**
     * {@code [slotCount][len+1][value]...} — the ColumNAR codec's payload, which carries its own count and writes
     * no companion field at all. See {@link ColumnarBinaryDocValuesField} for why the count has to travel inside
     * the blob.
     */
    COLUMNAR_PAYLOAD
}
