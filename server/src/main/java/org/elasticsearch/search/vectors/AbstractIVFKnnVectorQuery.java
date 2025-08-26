/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import com.carrotsearch.hppc.IntHashSet;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be at least 1, got: " + k);
        }
        if (visitRatio < 0.0f || visitRatio > 1.0f) {
            throw new IllegalArgumentException("visitRatio must be between 0.0 and 1.0 (both inclusive), got: " + visitRatio);
        }
        if (numCands < k) {
            throw new IllegalArgumentException("numCands must be at least k, got: " + numCands);
        }
        this.field = field;
        this.providedVisitRatio = visitRatio;
        this.k = k;
        this.filter = filter;
        this.numCands = numCands;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractIVFKnnVectorQuery that = (AbstractIVFKnnVectorQuery) o;
        return k == that.k
            && Objects.equals(field, that.field)
            && Objects.equals(filter, that.filter)
            && Objects.equals(providedVisitRatio, that.providedVisitRatio);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, filter, providedVisitRatio);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        vectorOpsCount = 0;
        IndexReader reader = indexSearcher.getIndexReader();

        final Weight filterWeight;
        Query rewrittenFilter = null;
        if (filter != null) {
            rewrittenFilter = filter.rewrite(indexSearcher);
            if (rewrittenFilter.getClass() == MatchNoDocsQuery.class) {
                return rewrittenFilter;
            }
            if (rewrittenFilter.getClass() != MatchAllDocsQuery.class) {
                BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
                    .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                    .build();
                rewrittenFilter = indexSearcher.rewrite(booleanQuery);
                if (rewrittenFilter.getClass() == MatchNoDocsQuery.class) {
                    return rewrittenFilter;
                }
                filterWeight = rewrittenFilter.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
            } else {
                filterWeight = null;
            }
        } else {
            filterWeight = null;
        }

        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();

        assert this instanceof IVFKnnFloatVectorQuery;
        int totalVectors = 0;
        double filterCost = 0;
        for (LeafReaderContext leafReaderContext : leafReaderContexts) {
            LeafReader leafReader = leafReaderContext.reader();
            FloatVectorValues floatVectorValues = leafReader.getFloatVectorValues(field);
            if (floatVectorValues != null) {
                totalVectors += floatVectorValues.size();
            }
            if (filterWeight != null) {
                ScorerSupplier supplier = filterWeight.scorerSupplier(leafReaderContext);
                if (supplier != null) {
                    filterCost += supplier.cost();
                }
            }
        }
        // account for gross misestimates of the iterator cost
        filterCost = Math.min(filterCost, totalVectors);

        // the filter is very permissive, treat it as a post filter
        float filterRatio = (float) filterCost / totalVectors;
        boolean overFetch = filterWeight != null && filterRatio > 0.9f;
        final float visitRatio;
        if (providedVisitRatio == 0.0f) {
            // dynamically set the percentage
            float expected = (float) Math.round(
                Math.log10(totalVectors) * Math.log10(totalVectors) * (Math.min(10_000, Math.max(numCands, 5 * k)))
            );
            visitRatio = overFetch ? (expected / totalVectors) / filterRatio : expected / totalVectors;
        } else {
            visitRatio = overFetch ? providedVisitRatio / filterRatio : providedVisitRatio;
        }
        // we need to ensure we are getting at least 2*k results to ensure we cover overspill duplicates
        // TODO move the logic for automatically adjusting percentages to the query, so we can only pass
        // 2k to the collector.
        int kToCollect = overFetch ? (int) (Math.round(2f * k) / filterRatio + 1f) : Math.round((2f * k));
        KnnCollectorManager knnCollectorManager = getKnnCollectorManager(kToCollect, indexSearcher);
        List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
        for (LeafReaderContext context : leafReaderContexts) {
            // very broad filter, don't bother filtering, just over fetch and apply the filter later
            if (overFetch) {
                // If the filter is more expensive than the visit ratio, skip it
                tasks.add(() -> searchLeaf(context, null, knnCollectorManager, visitRatio));
                continue;
            }
            tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager, visitRatio));
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

        // Merge sort the results
        TopDocs topK = TopDocs.merge(k, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return new MatchNoDocsQuery();
        }
        if (overFetch) {
            return new BooleanQuery.Builder().add(rewrittenFilter, BooleanClause.Occur.FILTER)
                .add(new KnnScoreDocQuery(topK.scoreDocs, reader), BooleanClause.Occur.MUST)
                .build();
        }
        return new KnnScoreDocQuery(topK.scoreDocs, reader);
    }

    private TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight, KnnCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        TopDocs results = getLeafResults(ctx, filterWeight, knnCollectorManager, visitRatio);
        IntHashSet dedup = new IntHashSet(results.scoreDocs.length * 4 / 3);
        int deduplicateCount = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (dedup.add(scoreDoc.doc)) {
                deduplicateCount++;
            }
        }
        ScoreDoc[] deduplicatedScoreDocs = new ScoreDoc[deduplicateCount];
        dedup.clear();
        int index = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (dedup.add(scoreDoc.doc)) {
                scoreDoc.doc += ctx.docBase;
                deduplicatedScoreDocs[index++] = scoreDoc;
            }
        }
        return new TopDocs(results.totalHits, deduplicatedScoreDocs);
    }

    TopDocs getLeafResults(LeafReaderContext ctx, Weight filterWeight, KnnCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();

        if (filterWeight == null) {
            return approximateSearch(ctx, liveDocs, knnCollectorManager, visitRatio);
        }

        Scorer scorer = filterWeight.scorer(ctx);
        if (scorer == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }

        BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, reader.maxDoc());
        return approximateSearch(ctx, acceptDocs, knnCollectorManager, visitRatio);
    }

    abstract TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        KnnCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException;

    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k);
    }

    @Override
    public final void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) throws IOException {
        if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
            // If we already have a BitSet and no deletions, reuse the BitSet
            return bitSetIterator.getBitSet();
        } else {
            // Create a new BitSet from matching and live docs
            FilteredDocIdSetIterator filterIterator = new FilteredDocIdSetIterator(iterator) {
                @Override
                protected boolean match(int doc) {
                    return liveDocs == null || liveDocs.get(doc);
                }
            };
            return BitSet.of(filterIterator, maxDoc);
        }
    }

    static class IVFCollectorManager implements KnnCollectorManager {
        private final int k;

        IVFCollectorManager(int k) {
            this.k = k;
        }

        @Override
        public KnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context) throws IOException {
            return new TopKnnCollector(k, visitedLimit, searchStrategy);
        }
    }
}
