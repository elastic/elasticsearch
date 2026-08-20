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
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query matching any {@code key\0value} slot that starts with the given {@code key\0} prefix, against a field stored in the
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull KeyedArrayOrderInlineNull}
 * binary doc-values format used by flattened fields in columnar mode.
 */
public class KeyedArrayOrderInlineNullPrefixQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef prefix;

    public KeyedArrayOrderInlineNullPrefixQuery(String fieldName, BytesRef prefix) {
        super(fieldName, slot -> startsWith(slot, prefix), true);
        this.prefix = Objects.requireNonNull(prefix);
    }

    private static boolean startsWith(BytesRef slot, BytesRef prefix) {
        return slot.length >= prefix.length
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
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
        assert counts != null : "KeyedArrayOrderInlineNull always writes a companion count field";
        return keyedInlineNullIterator(values, counts, matcher, matchCost);
    }

    @Override
    protected float matchCost() {
        return 10; // one prefix comparison per slot
    }

    @Override
    public String toString(String field) {
        return "KeyedArrayOrderInlineNullPrefixQuery(fieldName=" + fieldName + ",prefix=" + prefix.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        KeyedArrayOrderInlineNullPrefixQuery that = (KeyedArrayOrderInlineNullPrefixQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, prefix);
    }
}
