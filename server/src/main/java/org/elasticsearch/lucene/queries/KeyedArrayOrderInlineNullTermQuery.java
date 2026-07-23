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
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query for matching an exact {@code key\0value} term against a field stored in the
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull KeyedArrayOrderInlineNull}
 * binary doc-values format used by flattened fields in columnar mode. The term always carries the
 * {@code key\0} prefix, so it is never empty, and this query always needs the keyed-inline-null scan;
 * unlike {@link ScanningBinaryDocValuesTermQuery} there is no fast-path/rewrite to fall back to.
 */
public class KeyedArrayOrderInlineNullTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef term;

    public KeyedArrayOrderInlineNullTermQuery(String fieldName, BytesRef term) {
        super(fieldName, term::equals, true);
        this.term = Objects.requireNonNull(term);
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
        return 10; // because one comparison
    }

    @Override
    public String toString(String field) {
        return "KeyedArrayOrderInlineNullTermQuery(fieldName=" + fieldName + ",term=" + term.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        KeyedArrayOrderInlineNullTermQuery that = (KeyedArrayOrderInlineNullTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
