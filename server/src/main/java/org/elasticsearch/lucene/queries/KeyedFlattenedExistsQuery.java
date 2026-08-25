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
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Exists query for a single flattened sub-field ({@code my_flat.some_key: *}) whose values live in the
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull KeyedArrayOrderInlineNull}
 * binary doc-values encoding.
 *
 * <p>On columnar segments ({@link ColumnarKeyedBinaryDocValues}) the key is resolved to its segment ordinal once per leaf,
 * the leaf is skipped entirely when the key is absent, and per-document work amounts to a forward scan of one column
 * checking for any non-null slot. A document whose only slot for this key is {@code null} does <em>not</em> match,
 * consistent with the row-format behaviour in
 * {@link AbstractBinaryDocValuesQuery#keyedInlineNullIterator} (which guards the predicate with a null prefix check).
 *
 * <p>On row-format segments the fall back is {@link AbstractBinaryDocValuesQuery#keyedInlineNullIterator} with a
 * {@code key\0} prefix predicate — identical to the behaviour this class replaced.
 *
 * <p>See {@link KeyedFlattenedTermQuery} for the rationale on why per-segment layout dispatch is necessary.
 */
public class KeyedFlattenedExistsQuery extends AbstractBinaryDocValuesQuery {

    /** Key bytes without the {@code \0} separator, passed to {@link ColumnarKeyedBinaryDocValues#lookupKeyOrdinal}. */
    private final BytesRef key;

    /**
     * @param fieldName the {@code ._keyed} field name
     * @param key       the flattened sub-field key, without the {@code \0} separator
     */
    public KeyedFlattenedExistsQuery(String fieldName, String key) {
        super(fieldName, buildPrefixMatcher(key), true);
        this.key = new BytesRef(Objects.requireNonNull(key));
    }

    private static Predicate<BytesRef> buildPrefixMatcher(String key) {
        Objects.requireNonNull(key);
        // Row encoding stores slots as key\0value; any slot starting with key\0 belongs to this sub-field.
        final BytesRef prefix = new BytesRef(key + "\0");
        return slot -> slot.length >= prefix.length
            && Arrays.equals(
                slot.bytes,
                slot.offset,
                slot.offset + prefix.length,
                prefix.bytes,
                prefix.offset,
                prefix.offset + prefix.length
            );
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
            return columnarIterator(columnar, keyOrdinal, matchCost);
        }
        // Row layout: the whole key\0value slot list is one blob; the .counts companion drives iteration.
        final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
        assert counts != null : "KeyedArrayOrderInlineNull always writes a companion count field";
        return keyedInlineNullIterator(values, counts, matcher, matchCost);
    }

    /**
     * Two-phase iterator over one column. A document matches if it has at least one non-null slot for this key.
     * A document whose only slot is a null slot does not match, consistent with the row-format exists behaviour.
     *
     * <p>All {@code slotCount} slots are always consumed to keep the column cursor's payload pointer synchronised.
     */
    private static DocIdSetIterator columnarIterator(ColumnarKeyedBinaryDocValues columnar, int keyOrdinal, float cost) {
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(columnar) {
            @Override
            public boolean matches() throws IOException {
                final int slotCount = columnar.advanceExactKey(keyOrdinal);
                if (slotCount == 0) {
                    return false;
                }
                boolean hasNonNull = false;
                for (int slot = 0; slot < slotCount; slot++) {
                    final BytesRef slotValue = columnar.nextKeyValue();
                    if (hasNonNull == false && slotValue != null) {
                        hasNonNull = true;
                    }
                }
                return hasNonNull;
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
        return "KeyedFlattenedExistsQuery(fieldName=" + fieldName + ",key=" + key + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        KeyedFlattenedExistsQuery that = (KeyedFlattenedExistsQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, key);
    }
}
