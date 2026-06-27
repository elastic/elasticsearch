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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query for matching an exact BytesRef value for a specific field.
 * The equivalent of {@link org.apache.lucene.document.SortedDocValuesField#newSlowExactQuery(String, BytesRef)},
 * but then for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class ScanningBinaryDocValuesTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef term;

    public ScanningBinaryDocValuesTermQuery(String fieldName, BytesRef term) {
        super(fieldName, term::equals);
        this.term = Objects.requireNonNull(term);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (term.length == 0) {
            return new BinaryDocValuesLengthQuery(fieldName, 0);
        }
        String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            BinaryDocValues values = ctx.reader().getBinaryDocValues(fieldName);
            if (values == null) {
                continue; // field absent from this leaf — no docs to match, no constraint
            }
            DocValuesSkipper countsSkipper = ctx.reader().getDocValuesSkipper(countsFieldName);
            if (countsSkipper != null && countsSkipper.maxValue() > 1) {
                return this; // truly multi-valued leaf — optimized path not safe
            }
            if (!(values instanceof BlockLoader.OptionalColumnAtATimeReader direct) || direct.tryTermEqualIterator(term) == null) {
                return this; // format does not support the optimized two-phase iterator
            }
        }
        return new BinaryDocValuesTermEqualQuery(fieldName, term);
    }

    @Override
    protected float matchCost() {
        return 10; // because one comparison
    }

    @Override
    public String toString(String field) {
        return "ScanningBinaryDocValuesTermQuery(fieldName=" + fieldName + ",term=" + term.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesTermQuery that = (ScanningBinaryDocValuesTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, term);
    }
}
