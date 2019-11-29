/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.SparseFixedBitSet;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches only the first <code>topN</code> documents
 * sorted by a {@link LongPoint} field and discards the other ones.
 * This query can be used to retrieve the topN documents sorted by
 * a field efficiently when there is no other clause in the query.
 */
class SortedLongQuery extends Query {
    private final String field;
    private final long from;
    private final int fromMinDoc;
    private final int size;

    SortedLongQuery(String field, int size) {
        this(field, size, Long.MIN_VALUE, Integer.MAX_VALUE);
    }

    SortedLongQuery(String field, int size, long from, int fromMinDoc) {
        this.field = Objects.requireNonNull(field);
        this.from = from;
        this.fromMinDoc = fromMinDoc;
        this.size = size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), from, fromMinDoc, size);
    }


    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) {
            return false;
        }
        SortedLongQuery that = (SortedLongQuery) o;
        return from == that.from &&
            fromMinDoc == that.fromMinDoc &&
            size == that.size &&
            field.equals(that.field);
    }

    @Override
    public String toString(String field) {
        return "SortedLongQuery(" +
            "field='" + field + '\'' +
            ", from=" + from +
            ", fromMinDoc=" + fromMinDoc +
            ", size=" + size +
            ')';
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final PointValues pointValues = context.reader().getPointValues(field);
                if (pointValues == null) {
                    return null;
                }
                final Bits liveDocs = context.reader().getLiveDocs();
                byte[] minBytes = new byte[Long.SIZE];
                LongPoint.encodeDimension(from, minBytes, 0);
                final SparseFixedBitSet bitSet = new SparseFixedBitSet(context.reader().numDocs());
                final int[] count = new int[1];
                try {
                    pointValues.intersect(new PointValues.IntersectVisitor() {
                        @Override
                        public void visit(int docID) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void visit(int docID, byte[] packedValue) {
                            int cmp = FutureArrays.compareUnsigned(packedValue, 0, Long.BYTES, minBytes, 0, Long.BYTES);
                            if (cmp == 0 && fromMinDoc >= docID + context.docBase) {
                                return;
                            } else if (cmp < 0) {
                                return;
                            }
                            if (liveDocs == null || liveDocs.get(docID)) {
                                bitSet.set(docID);
                                if (++count[0] == size) {
                                    throw new CollectionTerminatedException();
                                }
                            }
                        }

                        @Override
                        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                            if (FutureArrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, minBytes, 0, Long.BYTES) < 0) {
                                return PointValues.Relation.CELL_OUTSIDE_QUERY;
                            }
                            return PointValues.Relation.CELL_CROSSES_QUERY;
                        }
                    });
                } catch (CollectionTerminatedException exc) {}
                if (count[0] == 0) {
                    return null;
                }
                return new ConstantScoreScorer(this, score(), scoreMode, new BitSetIterator(bitSet, count[0]));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }
}
