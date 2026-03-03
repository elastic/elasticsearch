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
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

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

                final NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
                final DocIdSetIterator iterator;

                // TODO This class would be simpler if it were a subclass of AbstractBinaryDocValuesQuery, but we
                // intend to optimize the single value case with a `containsIterator` which pushes matching down to
                // the codec. Since a separate class will then be needed, we will start with a standalone class.
                Predicate<BytesRef> predicate = bytes -> contains(bytes, containsTerm);
                iterator = TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(counts) {
                    final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

                    @Override
                    public boolean matches() throws IOException {
                        values.advance(counts.docID());
                        return reader.match(values.binaryValue(), counts.longValue(), predicate);
                    }

                    @Override
                    public float matchCost() {
                        return matchCost;
                    }
                });

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

    // Copied and modified from StringUTF16.indexOfLatin1Unsafe
    public static boolean contains(BytesRef value, BytesRef term) {
        byte first = term.bytes[term.offset];
        int max = (value.length - term.length) + value.offset;
        for (int i = value.offset; i <= max; i++) {
            // Look for first character.
            if (value.bytes[i] != first) {
                while (++i <= max && value.bytes[i] != first)
                    ;
            }
            // Found first character, now look at the rest of value
            if (i <= max) {
                int j = i + 1;
                int end = j + term.length - 1;
                for (int k = 1; j < end && value.bytes[j] == term.bytes[k]; j++, k++)
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
