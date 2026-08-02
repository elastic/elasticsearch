/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Exact-term query for a single flattened sub-field ({@code my_flat.some_key: value}) whose values live in the
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull KeyedArrayOrderInlineNull}
 * binary doc-values encoding.
 *
 * <p>Why this exists rather than a plain blob scan: when the {@code ._keyed} field is stored by
 * {@link org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat} the per-document blob is not materialised on
 * disk at all — {@link BinaryDocValues#binaryValue()} has to transpose <em>every</em> column back into a blob,
 * paying O(numKeys) per candidate document. This query instead detects {@link ColumnarKeyedBinaryDocValues} per leaf,
 * resolves the key to its segment ordinal once, and then touches only that one column. When the key does not occur
 * anywhere in the segment the whole leaf is skipped without any per-document work.
 *
 * <p>The layout is a per-segment property, not a mapping property: an index can hold both columnar and row segments
 * (mapping gained {@code layout: columnar} after some segments were written, {@code es.flattened.mergeColumnWise=false}
 * set, or a merge that mixed sources). The dispatch happens per {@link LeafReaderContext} inside
 * {@link #getDocIdSetIterator}, exactly as {@code FlattenedFieldMapper.loadArrayOrder} already does. On a non-columnar
 * leaf this falls back to {@link AbstractBinaryDocValuesQuery#keyedInlineNullIterator}, which is the behaviour this
 * class replaced.
 */
public class KeyedFlattenedTermQuery extends AbstractBinaryDocValuesQuery {

    /** {@code key\0value} — the form stored in each slot of the row (KeyedArrayOrderInlineNull) encoding. */
    private final BytesRef term;
    /** Key bytes without the {@code \0} separator, which is what {@link ColumnarKeyedBinaryDocValues#lookupKeyOrdinal} expects. */
    private final BytesRef key;
    /** Value bytes only; a slice of {@link #term}. Columnar columns store values without the key prefix. */
    private final BytesRef value;

    /**
     * @param fieldName the {@code ._keyed} field name
     * @param key       the flattened sub-field key, without the {@code \0} separator
     * @param keyedTerm the full {@code key\0value} term as produced by
     *                  {@code FlattenedFieldMapper.KeyedFlattenedFieldType#indexedValueForSearch}
     */
    public KeyedFlattenedTermQuery(String fieldName, String key, BytesRef keyedTerm) {
        super(fieldName, keyedTerm::equals, true);
        this.term = Objects.requireNonNull(keyedTerm);
        this.key = new BytesRef(Objects.requireNonNull(key));
        final int prefixLength = this.key.length + 1; // key bytes + \0
        if (keyedTerm.length < prefixLength) {
            throw new IllegalArgumentException("term [" + keyedTerm + "] is not prefixed by key [" + key + "]");
        }
        assert keyedTerm.bytes[keyedTerm.offset + this.key.length] == 0 : "term is not key\\0value encoded";
        // Slice the value bytes out of the combined term; no re-encode needed.
        this.value = new BytesRef(keyedTerm.bytes, keyedTerm.offset + prefixLength, keyedTerm.length - prefixLength);
    }

    @Override
    protected DocIdSetIterator getDocIdSetIterator(LeafReaderContext context, float matchCost) throws IOException {
        final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        if (values instanceof ColumnarKeyedBinaryDocValues columnar) {
            final int keyOrdinal = columnar.lookupKeyOrdinal(key);
            if (keyOrdinal < 0) {
                // The sub-field has no column in this segment; skip the whole leaf.
                return null;
            }
            return columnarIterator(columnar, keyOrdinal, value, matchCost);
        }
        // Row layout: the whole key\0value slot list is one blob; the .counts companion drives iteration.
        final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
        assert counts != null : "KeyedArrayOrderInlineNull always writes a companion count field";
        return keyedInlineNullIterator(values, counts, matcher, matchCost);
    }

    /**
     * Two-phase iterator over one column. The approximation is the columnar reader itself — it is the
     * docs-with-field {@link DocIdSetIterator} with an exact {@code cost()}, and its {@code nextDoc()}/{@code advance()}
     * set the current document ID that {@link ColumnarKeyedBinaryDocValues#advanceExactKey} reads.
     * Using it avoids opening the {@code .counts} numeric DV column entirely.
     *
     * <p>All {@code slotCount} slots are always consumed even after a match: leaving slots unread
     * desynchronises the column cursor's payload pointer and silently corrupts reads for all later docs
     * in the same decompressed block. Only the comparison is short-circuited.
     */
    private static DocIdSetIterator columnarIterator(ColumnarKeyedBinaryDocValues columnar, int keyOrdinal, BytesRef value, float cost) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(columnar) {
            @Override
            public boolean matches() throws IOException {
                final int slotCount = columnar.advanceExactKey(keyOrdinal);
                if (slotCount == 0) {
                    return false;
                }
                boolean matched = false;
                for (int slot = 0; slot < slotCount; slot++) {
                    final BytesRef slotValue = columnar.nextKeyValue();
                    // null is a null slot; a null can never equal a term.
                    if (matched == false && slotValue != null && value.bytesEquals(slotValue)) {
                        matched = true;
                    }
                }
                return matched;
            }

            @Override
            public float matchCost() {
                return cost;
            }
        });
    }

    @Override
    protected float matchCost() {
        return 10;
    }

    @Override
    public String toString(String field) {
        return "KeyedFlattenedTermQuery(fieldName=" + fieldName + ",term=" + term + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        KeyedFlattenedTermQuery that = (KeyedFlattenedTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, key, value);
    }
}
