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

    // Copied and modified from StringUTF16.indexOfLatin1Unsafe
    public static boolean contains(byte[] value, int offset, int length, BytesRef term) {
        byte first = term.bytes[term.offset];
        int max = (length - term.length) + offset;
        for (int i = offset; i <= max; i++) {
            // Look for first character.
            if (value[i] != first) {
                while (++i <= max && value[i] != first)
                    ;
            }
            // Found first character, now look at the rest of value
            if (i <= max) {
                int j = i + 1;
                int end = j + term.length - 1;
                for (int k = term.offset + 1; j < end && value[j] == term.bytes[k]; j++, k++)
                    ;
                if (j == end) {
                    // Found whole string.
                    return true;
                }
            }
        }
        return false;
    }
}
