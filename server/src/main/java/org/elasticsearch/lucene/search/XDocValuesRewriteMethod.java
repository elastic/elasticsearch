/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Fork of Lucene 10.5.0's {@code DocValuesRewriteMethod}, using {@link XDocValuesRangeIterator}
 * for {@code forOrdinalSet} so that {@code docIDRunEnd()} returns a correct, conservative run
 * boundary on {@code MAYBE} and {@code YES_IF_PRESENT} blocks.
 *
 * <p>The upstream fix is in apache/lucene#16450, is available in Lucene 10.5.1. Delete this
 * class, revert callers to {@link MultiTermQuery#DOC_VALUES_REWRITE}, and remove the
 * {@link #DOC_VALUES_REWRITE} singleton once Elasticsearch upgrades to a Lucene release containing
 * that fix.
 */
public final class XDocValuesRewriteMethod extends MultiTermQuery.RewriteMethod {

    static {
        assert Version.LUCENE_10_5_0.onOrAfter(Version.LATEST) : "Remove this class as fix is part of 10.5.1 and later";
    }

    /** Drop-in replacement for {@link MultiTermQuery#DOC_VALUES_REWRITE}. */
    public static final MultiTermQuery.RewriteMethod DOC_VALUES_REWRITE = new XDocValuesRewriteMethod();

    private XDocValuesRewriteMethod() {}

    @Override
    public Query rewrite(IndexSearcher indexSearcher, MultiTermQuery query) {
        return new XMultiTermQueryDocValuesWrapper(query);
    }

    @Override
    public boolean equals(Object other) {
        return other != null && getClass() == other.getClass();
    }

    @Override
    public int hashCode() {
        return 641;
    }

    static class XMultiTermQueryDocValuesWrapper extends Query {

        protected final MultiTermQuery query;

        protected XMultiTermQueryDocValuesWrapper(MultiTermQuery query) {
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return query.toString(field);
        }

        @Override
        public final boolean equals(final Object other) {
            return sameClassAs(other) && query.equals(((XMultiTermQueryDocValuesWrapper) other).query);
        }

        @Override
        public final int hashCode() {
            return 31 * classHash() + query.hashCode();
        }

        public final String getField() {
            return query.getField();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(query.getField())) {
                visitor.getSubVisitor(BooleanClause.Occur.FILTER, query);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {

                private TermsEnum getTermsEnum(SortedSetDocValues values) throws IOException {
                    return query.getTermsEnum(new Terms() {

                        @Override
                        public TermsEnum iterator() throws IOException {
                            return values.termsEnum();
                        }

                        @Override
                        public long getSumTotalTermFreq() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long getSumDocFreq() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public int getDocCount() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long size() {
                            return -1;
                        }

                        @Override
                        public boolean hasFreqs() {
                            return false;
                        }

                        @Override
                        public boolean hasOffsets() {
                            return false;
                        }

                        @Override
                        public boolean hasPositions() {
                            return false;
                        }

                        @Override
                        public boolean hasPayloads() {
                            return false;
                        }
                    });
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), query.getField());
                    if (values.getValueCount() == 0) {
                        return null;
                    }

                    return new ScorerSupplier() {
                        @Override
                        public ConstantScoreScorer get(long leadCost) throws IOException {
                            TermsEnum termsEnum = getTermsEnum(values);
                            assert termsEnum != null;
                            DocValuesSkipper skipper = context.reader().getDocValuesSkipper(query.getField());
                            SortedDocValues singleton = DocValues.unwrapSingleton(values);
                            return singleton == null
                                ? new ConstantScoreScorer(
                                    score(),
                                    scoreMode,
                                    XDocValuesRangeIterator.forOrdinalSet(values, skipper, termsEnum)
                                )
                                : new ConstantScoreScorer(
                                    score(),
                                    scoreMode,
                                    XDocValuesRangeIterator.forOrdinalSet(singleton, skipper, termsEnum)
                                );
                        }

                        @Override
                        public long cost() {
                            return values.cost();
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return DocValues.isCacheable(ctx, query.getField());
                }
            };
        }
    }
}
