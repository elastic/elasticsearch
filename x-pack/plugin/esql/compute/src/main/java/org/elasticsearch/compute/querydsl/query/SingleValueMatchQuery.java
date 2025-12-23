/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.querydsl.query;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;

import java.io.IOException;
import java.util.Objects;

/**
 * Finds all fields with a single-value. If a field has a multi-value, it emits
 * a {@link Warnings warning}.
 * <p>
 *     Warnings are only emitted if the {@link TwoPhaseIterator#matches}. Meaning that,
 *     if the other query skips the doc either because the index doesn't match or because it's
 *     {@link TwoPhaseIterator#matches} doesn't match, then we won't log warnings. So it's
 *     most safe to say that this will emit a warning if the document would have
 *     matched but for having a multivalued field. If the document doesn't match but
 *     "almost" matches in some fairly lucene-specific ways then it *might* emit
 *     a warning.
 * </p>
 */
public final class SingleValueMatchQuery extends Query {

    /**
     * Choose a big enough value so this approximation never drives the iteration.
     * This avoids reporting warnings when queries are not matching multi-values
     */
    private static final int MULTI_VALUE_MATCH_COST = 1000;
    private final IndexFieldData<?> fieldData;
    private final Warnings warnings;
    private final String multiValueExceptionMessage;

    public SingleValueMatchQuery(IndexFieldData<?> fieldData, Warnings warnings, String multiValueExceptionMessage) {
        this.fieldData = fieldData;
        this.warnings = warnings;
        this.multiValueExceptionMessage = multiValueExceptionMessage;
    }

    @Override
    public String toString(String field) {
        StringBuilder builder = new StringBuilder("single_value_match(");
        if (false == this.fieldData.getFieldName().equals(field)) {
            builder.append(this.fieldData.getFieldName());
        }
        return builder.append(")").toString();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafFieldData lfd = fieldData.load(context);
                if (lfd == null) {
                    return null;
                }
                /*
                 * SortedBinaryDocValues are available for most fields, but they
                 * are made available by eagerly converting non-bytes values to
                 * utf-8 strings. The eager conversion is quite expensive. So
                 * we specialize on numeric fields and fields with ordinals to
                 * avoid that expense in at least that case.
                 *
                 * Also! Lucene's FieldExistsQuery only needs one scorer that can
                 * use all the docs values iterators at DocIdSetIterators. We
                 * can't do that because we need the check the number of fields.
                 */
                if (lfd instanceof LeafNumericFieldData n) {
                    return scorerSupplier(context, n.getLongValues(), boost, scoreMode);
                }
                if (lfd instanceof LeafOrdinalsFieldData o) {
                    return scorerSupplier(context, o.getOrdinalsValues(), boost, scoreMode);
                }
                return scorerSupplier(context, lfd.getBytesValues(), boost, scoreMode);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                final LeafFieldData lfd = fieldData.load(ctx);
                // If field is singleton, then it is safe to cache this query, because no warning will ever be emitted.
                if (lfd instanceof LeafNumericFieldData n) {
                    if (SortedNumericLongValues.unwrapSingleton(n.getLongValues()) != null) {
                        return true;
                    }
                } else if (lfd instanceof LeafOrdinalsFieldData o) {
                    if (DocValues.unwrapSingleton(o.getOrdinalsValues()) != null) {
                        return true;
                    }
                }
                // don't cache so we can emit warnings
                return false;
            }

            private ScorerSupplier scorerSupplier(
                LeafReaderContext context,
                SortedNumericLongValues sortedNumerics,
                float boost,
                ScoreMode scoreMode
            ) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                NumericDocValues ndv = DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), fieldData.getFieldName()));
                if (ndv != null && ndv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    ndv = DocValues.unwrapSingleton(DocValues.getSortedNumeric(context.reader(), fieldData.getFieldName()));
                    return new DocIdSetIteratorScorerSupplier(boost, scoreMode, ndv);
                }
                final CheckedIntPredicate predicate = doc -> {
                    if (false == sortedNumerics.advanceExact(doc)) {
                        return false;
                    }
                    if (sortedNumerics.docValueCount() != 1) {
                        registerMultiValueException();
                        return false;
                    }
                    return true;
                };
                return new PredicateScorerSupplier(boost, scoreMode, maxDoc, MULTI_VALUE_MATCH_COST, predicate);
            }

            private ScorerSupplier scorerSupplier(
                LeafReaderContext context,
                SortedSetDocValues sortedSetDocValues,
                float boost,
                ScoreMode scoreMode
            ) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                SortedDocValues sdv = DocValues.unwrapSingleton(DocValues.getSortedSet(context.reader(), fieldData.getFieldName()));
                if (sdv != null && sdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    sdv = DocValues.unwrapSingleton(DocValues.getSortedSet(context.reader(), fieldData.getFieldName()));
                    return new DocIdSetIteratorScorerSupplier(boost, scoreMode, sdv);
                }
                final CheckedIntPredicate predicate = doc -> {
                    if (false == sortedSetDocValues.advanceExact(doc)) {
                        return false;
                    }
                    if (sortedSetDocValues.docValueCount() != 1) {
                        registerMultiValueException();
                        return false;
                    }
                    return true;
                };
                return new PredicateScorerSupplier(boost, scoreMode, maxDoc, MULTI_VALUE_MATCH_COST, predicate);
            }

            private ScorerSupplier scorerSupplier(
                LeafReaderContext context,
                SortedBinaryDocValues sortedBinaryDocValues,
                float boost,
                ScoreMode scoreMode
            ) {
                final int maxDoc = context.reader().maxDoc();
                if (FieldData.unwrapSingleton(sortedBinaryDocValues) != null) {
                    return new PredicateScorerSupplier(
                        boost,
                        scoreMode,
                        maxDoc,
                        MULTI_VALUE_MATCH_COST,
                        sortedBinaryDocValues::advanceExact
                    );
                }
                final CheckedIntPredicate predicate = doc -> {
                    if (false == sortedBinaryDocValues.advanceExact(doc)) {
                        return false;
                    }
                    if (sortedBinaryDocValues.docValueCount() != 1) {
                        registerMultiValueException();
                        return false;
                    }
                    return true;
                };
                return new PredicateScorerSupplier(boost, scoreMode, maxDoc, MULTI_VALUE_MATCH_COST, predicate);
            }
        };
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (fieldData instanceof ConstantIndexFieldData cfd && cfd.getValue() != null) {
            return Queries.ALL_DOCS_INSTANCE;
        }
        for (LeafReaderContext context : indexSearcher.getIndexReader().leaves()) {
            final LeafReader reader = context.reader();
            final int maxDoc = reader.maxDoc();
            final LeafFieldData lfd = fieldData.load(context);
            if (lfd instanceof LeafNumericFieldData) {
                NumericDocValues singleton = DocValues.unwrapSingleton(reader.getSortedNumericDocValues(fieldData.getFieldName()));
                if (singleton != null) {
                    singleton.nextDoc();
                    if (singleton.docIDRunEnd() == maxDoc) {
                        continue;
                    }
                }
                // TODO: check doc values skippers
                final PointValues points = reader.getPointValues(fieldData.getFieldName());
                if (points != null && points.getDocCount() == maxDoc && points.size() == points.getDocCount()) {
                    continue;
                }
                return super.rewrite(indexSearcher);
            } else if (lfd instanceof LeafOrdinalsFieldData) {
                SortedDocValues singleton = DocValues.unwrapSingleton(reader.getSortedSetDocValues(fieldData.getFieldName()));
                if (singleton != null) {
                    singleton.nextDoc();
                    if (singleton.docIDRunEnd() == maxDoc) {
                        continue;
                    }
                }
                // TODO: check doc values skippers
                Terms terms = reader.terms(fieldData.getFieldName());
                if (terms != null && terms.getDocCount() == maxDoc && terms.getSumDocFreq() == terms.getDocCount()) {
                    continue;
                }
                return super.rewrite(indexSearcher);
            } else {
                return super.rewrite(indexSearcher);
            }
        }
        return Queries.ALL_DOCS_INSTANCE;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldData.getFieldName())) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        final SingleValueMatchQuery other = (SingleValueMatchQuery) obj;
        return fieldData.getFieldName().equals(other.fieldData.getFieldName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldData.getFieldName());
    }

    private static class DocIdSetIteratorScorerSupplier extends ScorerSupplier {

        private final float score;
        private final ScoreMode scoreMode;
        private final DocIdSetIterator docIdSetIterator;

        private DocIdSetIteratorScorerSupplier(float score, ScoreMode scoreMode, DocIdSetIterator docIdSetIterator) {
            this.score = score;
            this.scoreMode = scoreMode;
            this.docIdSetIterator = docIdSetIterator;
        }

        @Override
        public Scorer get(long leadCost) {
            return new ConstantScoreScorer(score, scoreMode, docIdSetIterator);
        }

        @Override
        public long cost() {
            return docIdSetIterator.cost();
        }
    }

    private void registerMultiValueException() {
        warnings.registerException(IllegalArgumentException.class, multiValueExceptionMessage);
    }

    private static class PredicateScorerSupplier extends ScorerSupplier {
        private final float score;
        private final ScoreMode scoreMode;
        private final int maxDoc;
        private final int matchCost;
        private final CheckedIntPredicate predicate;

        private PredicateScorerSupplier(float score, ScoreMode scoreMode, int maxDoc, int matchCost, CheckedIntPredicate predicate) {
            this.score = score;
            this.scoreMode = scoreMode;
            this.maxDoc = maxDoc;
            this.matchCost = matchCost;
            this.predicate = predicate;
        }

        @Override
        public Scorer get(long leadCost) {
            TwoPhaseIterator iterator = new TwoPhaseIterator(DocIdSetIterator.all(maxDoc)) {
                @Override
                public boolean matches() throws IOException {
                    return predicate.test(approximation.docID());
                }

                @Override
                public float matchCost() {
                    return matchCost;
                }
            };
            return new ConstantScoreScorer(score, scoreMode, iterator);
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    @FunctionalInterface
    private interface CheckedIntPredicate {
        boolean test(int doc) throws IOException;
    }
}
