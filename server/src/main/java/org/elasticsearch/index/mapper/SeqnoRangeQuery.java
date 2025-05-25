/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

final class SeqnoRangeQuery extends Query {

    final long lowerValue;
    final long upperValue;

    SeqnoRangeQuery(long lowerValue, long upperValue) {
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
    }

    @Override
    public String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (SeqNoFieldMapper.NAME.equals(field) == false) {
            b.append(SeqNoFieldMapper.NAME).append(":");
        }
        return b.append("[").append(lowerValue).append(" TO ").append(upperValue).append("]").toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        var that = (SeqnoRangeQuery) obj;
        return lowerValue == that.lowerValue && upperValue == that.upperValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), lowerValue, upperValue);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
            return new FieldExistsQuery(SeqNoFieldMapper.NAME);
        }
        if (lowerValue > upperValue) {
            return new MatchNoDocsQuery();
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(SeqNoFieldMapper.NAME)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

        final Query indexQuery = LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, lowerValue, upperValue);
        final Query dvQuery = NumericDocValuesField.newSlowRangeQuery(SeqNoFieldMapper.NAME, lowerValue, upperValue);

        // Most of this logic originates from Lucene's IndexOrDocValueQuery:
        final Weight indexWeight = indexQuery.createWeight(searcher, scoreMode, boost);
        final Weight dvWeight = dvQuery.createWeight(searcher, scoreMode, boost);
        return new Weight(this) {
            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                return dvWeight.matches(context, doc);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return dvWeight.explain(context, doc);
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                PointValues pointValues = context.reader().getPointValues(SeqNoFieldMapper.NAME);
                if (pointValues != null) {
                    final int count = indexWeight.count(context);
                    if (count != -1) {
                        return count;
                    }
                    return dvWeight.count(context);
                } else {
                    return dvWeight.count(context);
                }
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                PointValues pointValues = context.reader().getPointValues(SeqNoFieldMapper.NAME);
                if (pointValues != null) {
                    final ScorerSupplier indexScorerSupplier = indexWeight.scorerSupplier(context);
                    final ScorerSupplier dvScorerSupplier = dvWeight.scorerSupplier(context);
                    if (indexScorerSupplier == null || dvScorerSupplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            // At equal costs, doc values tend to be worse than points since they
                            // still need to perform one comparison per document while points can
                            // do much better than that given how values are organized. So we give
                            // an arbitrary 8x penalty to doc values.
                            final long threshold = cost() >>> 3;
                            if (threshold <= leadCost) {
                                return indexScorerSupplier.get(leadCost);
                            } else {
                                return dvScorerSupplier.get(leadCost);
                            }
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            // Bulk scorers need to consume the entire set of docs, so using an
                            // index structure should perform better
                            return indexScorerSupplier.bulkScorer();
                        }

                        @Override
                        public long cost() {
                            return indexScorerSupplier.cost();
                        }
                    };
                } else {
                    return dvWeight.scorerSupplier(context);
                }
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
