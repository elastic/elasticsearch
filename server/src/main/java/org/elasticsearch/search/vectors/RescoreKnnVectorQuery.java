/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityFloatValueSource;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A Lucene {@link Query} that applies vector-based rescoring to an inner query's results.
 * <p>
 * Depending on the nature of the {@code innerQuery}, this class dynamically selects between two rescoring strategies:
 * <ul>
 *   <li><b>Inline rescoring</b>:
 *   Used when the inner query is already a top-N vector query with {@code rescoreK} results.
 *   The vector similarity is applied inline using a {@link FunctionScoreQuery} without an additional
 *   filtering pass.</li>
 *   <li><b>Late rescoring</b>: Used when the inner query is not a top-N vector query or does not return
 *   {@code rescoreK} results. The top {@code rescoreK} documents are first collected, and then rescoring is applied
 *   separately to select the final top {@code k}.</li>
 * </ul>
 */
public abstract class RescoreKnnVectorQuery extends Query implements QueryProfilerProvider {
    protected final String fieldName;
    protected final float[] floatTarget;
    protected final VectorSimilarityFunction vectorSimilarityFunction;
    protected final int k;
    protected final Query innerQuery;
    protected long vectorOperations = 0;

    private RescoreKnnVectorQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        int k,
        Query innerQuery
    ) {
        this.fieldName = fieldName;
        this.floatTarget = floatTarget;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
        this.k = k;
        this.innerQuery = innerQuery;
    }

    /**
     * Selects and returns the appropriate {@link RescoreKnnVectorQuery} strategy based on the nature of the {@code innerQuery}.
     *
     * @param fieldName                 the name of the field containing the vector
     * @param floatTarget              the target vector to compare against
     * @param vectorSimilarityFunction the similarity function to apply
     * @param k                        the number of top documents to return after rescoring
     * @param rescoreK                 the number of top documents to consider for rescoring
     * @param innerQuery               the original Lucene query to rescore
     * @param parentFilterProducer     A filter used to produce the BitSet identifying parent documents,
     *                                 required when the field is nested to determine the parent of each child.
     *                                 If {@code null}, the query is assumed to match parent documents directly.
     */
    public static RescoreKnnVectorQuery fromInnerQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        int k,
        int rescoreK,
        Query innerQuery,
        @Nullable BitSetProducer parentFilterProducer
    ) {
        if ((innerQuery instanceof KnnFloatVectorQuery fQuery && fQuery.getK() == rescoreK)
            || (innerQuery instanceof KnnByteVectorQuery bQuery && bQuery.getK() == rescoreK)
            || (innerQuery instanceof AbstractIVFKnnVectorQuery ivfQuery && ivfQuery.k == rescoreK)) {
            // We ignore the nested context here since the knn query already handles the parent diversification.
            return new InlineRescoreQuery(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
        }
        return new LateRescoreQuery(fieldName, floatTarget, vectorSimilarityFunction, k, rescoreK, innerQuery, parentFilterProducer);
    }

    public Query innerQuery() {
        return innerQuery;
    }

    public int k() {
        return k;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        if (innerQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            queryProfilerProvider.profile(queryProfiler);
        }

        queryProfiler.addVectorOpsCount(vectorOperations);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        innerQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescoreKnnVectorQuery that = (RescoreKnnVectorQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Arrays.equals(floatTarget, that.floatTarget)
            && vectorSimilarityFunction == that.vectorSimilarityFunction
            && Objects.equals(k, that.k)
            && Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(floatTarget), vectorSimilarityFunction, k, innerQuery);
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName()
            + "{"
            + "fieldName='"
            + fieldName
            + '\''
            + ", floatTarget="
            + floatTarget[0]
            + "..."
            + ", vectorSimilarityFunction="
            + vectorSimilarityFunction
            + ", k="
            + k
            + ", vectorQuery="
            + innerQuery
            + '}';
    }

    private static class InlineRescoreQuery extends RescoreKnnVectorQuery {
        private InlineRescoreQuery(
            String fieldName,
            float[] floatTarget,
            VectorSimilarityFunction vectorSimilarityFunction,
            int k,
            Query innerQuery
        ) {
            super(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            var valueSource = new VectorSimilarityFloatValueSource(fieldName, floatTarget, vectorSimilarityFunction);
            var functionScoreQuery = new FunctionScoreQuery(innerQuery, valueSource);
            // Retrieve top k documents from the function score query
            var topDocs = searcher.search(functionScoreQuery, k);
            vectorOperations = topDocs.totalHits.value();
            return new KnnScoreDocQuery(topDocs.scoreDocs, searcher.getIndexReader());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    private static class LateRescoreQuery extends RescoreKnnVectorQuery {
        final int rescoreK;
        final BitSetProducer parentFilter;

        private LateRescoreQuery(
            String fieldName,
            float[] floatTarget,
            VectorSimilarityFunction vectorSimilarityFunction,
            int k,
            int rescoreK,
            Query innerQuery,
            @Nullable BitSetProducer parentFilter
        ) {
            super(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
            this.rescoreK = rescoreK;
            this.parentFilter = parentFilter;
        }

        record DocAndParent(int parent, int doc) {}

        @Override
        @SuppressWarnings("unchecked")
        public Query rewrite(IndexSearcher searcher) throws IOException {
            final TopDocs topDocs;
            if (parentFilter != null) {
                // We're dealing with a nested field, so we need to search at the child level.
                // We retrieve the top `rescoreK` child documents, but collapse them so that only
                // the best child per parent is kept.
                FirstPassGroupingCollector<ParentAndDoc> groupingCollector = new FirstPassGroupingCollector<>(
                    new ParentSelector(parentFilter),
                    Sort.RELEVANCE,
                    rescoreK
                );
                searcher.search(innerQuery, groupingCollector);
                var groups = groupingCollector.getTopGroups(0);
                var scoreDocs = groups.stream().map(g -> new ScoreDoc(g.groupValue.doc, (float) g.sortValues[0])).toArray(ScoreDoc[]::new);
                topDocs = new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
            } else {
                // Retrieve top `rescoreK` documents from the inner query
                topDocs = searcher.search(innerQuery, rescoreK);
            }
            vectorOperations = topDocs.totalHits.value();

            // Retrieve top `k` documents from the top `rescoreK` query
            var topDocsQuery = new KnnScoreDocQuery(topDocs.scoreDocs, searcher.getIndexReader());
            var valueSource = new VectorSimilarityFloatValueSource(fieldName, floatTarget, vectorSimilarityFunction);
            var rescoreQuery = new FunctionScoreQuery(topDocsQuery, valueSource);
            var rescoreTopDocs = searcher.search(rescoreQuery.rewrite(searcher), k);
            return new KnnScoreDocQuery(rescoreTopDocs.scoreDocs, searcher.getIndexReader());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            var that = (RescoreKnnVectorQuery.LateRescoreQuery) o;
            return super.equals(o) && that.rescoreK == rescoreK;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rescoreK);
        }
    }

    private static class ParentAndDoc {
        int parent;
        int doc;

        ParentAndDoc(int parent, int doc) {
            this.parent = parent;
            this.doc = doc;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParentAndDoc that = (ParentAndDoc) o;
            return parent == that.parent;
        }

        @Override
        public int hashCode() {
            return Objects.hash(parent);
        }
    }

    private static class ParentSelector extends GroupSelector<ParentAndDoc> {
        final BitSetProducer parentFilter;
        BitSet parentBitSet;
        int currentParent = -1;
        ParentAndDoc current;

        ParentSelector(BitSetProducer parentFilter) {
            this.parentFilter = parentFilter;
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            parentBitSet = parentFilter.getBitSet(readerContext);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {}

        @Override
        public State advanceTo(int doc) throws IOException {
            if (parentBitSet == null) {
                return State.SKIP;
            }
            if (doc > currentParent) {
                currentParent = parentBitSet.nextSetBit(doc);
                current.parent = currentParent;
            }
            current.doc = doc;
            return State.ACCEPT;
        }

        @Override
        public ParentAndDoc currentValue() throws IOException {
            return current;
        }

        @Override
        public ParentAndDoc copyValue() throws IOException {
            return new ParentAndDoc(current.parent, current.doc);
        }

        @Override
        public void setGroups(Collection<SearchGroup<ParentAndDoc>> searchGroups) {}
    }
}
