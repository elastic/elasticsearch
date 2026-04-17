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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query that matches documents whose binary doc values contain a specific term. Only works with binary doc values
 * encoded using {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount}.
 */
public final class BinaryDocValuesContainsTermQuery extends Query {
    final String fieldName;
    final BytesRef containsTerm;

    BinaryDocValuesContainsTermQuery(String fieldName, BytesRef containsTerm) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.containsTerm = Objects.requireNonNull(containsTerm);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        float matchCost = matchCost();
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                final DocIdSetIterator containsIter = values instanceof BlockLoader.OptionalColumnAtATimeReader direct
                    ? direct.tryContainsIterator(containsTerm)
                    : null;

                final DocIdSetIterator iterator;
                if (containsIter != null) {
                    iterator = containsIter;
                } else {
                    Predicate<BytesRef> predicate = bytes -> contains(bytes, containsTerm);
                    String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
                    final NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
                    iterator = AbstractBinaryDocValuesQuery.multiValuedIterator(values, counts, predicate, matchCost());
                }
                return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, context.reader().maxDoc());
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    float matchCost() {
        return 10;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    public String toString(String field) {
        return "BinaryDocValuesContainsTermQuery(fieldName=" + field + ",containsTerm=" + containsTerm + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesContainsTermQuery that = (BinaryDocValuesContainsTermQuery) o;
        return Objects.equals(fieldName, that.fieldName) && Objects.equals(containsTerm, that.containsTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, containsTerm);
    }

    public static boolean contains(BytesRef value, BytesRef term) {
        return contains(value.bytes, value.offset, value.length, term);
    }

    public static boolean contains(byte[] value, int offset, int length, BytesRef term) {
        return ESVectorUtil.contains(value, offset, length, term.bytes, term.offset, term.length);
    }
}
