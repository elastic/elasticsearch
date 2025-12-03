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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAccumulator;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

abstract class AbstractIVFKnnVectorQuery extends Query implements QueryProfilerProvider {

    record VectorLeafSearchFilterMeta(LeafReaderContext context, AcceptDocs filter) {}

    static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

    protected final String field;
    protected final float providedVisitRatio;
    protected final int k;
    protected final int numCands;
    protected final Query filter;
    protected int vectorOpsCount;
    protected final float postFilteringThreshold;

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter) {
        this(field, visitRatio, k, numCands, filter, .75f);
    }

    protected AbstractIVFKnnVectorQuery(String field, float visitRatio, int k, int numCands, Query filter, float postFilteringThreshold) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be at least 1, got: " + k);
        }
        if (visitRatio < 0.0f || visitRatio > 1.0f) {
            throw new IllegalArgumentException("visitRatio must be between 0.0 and 1.0 (both inclusive), got: " + visitRatio);
        }
        if (numCands < k) {
            throw new IllegalArgumentException("numCands must be at least k, got: " + numCands);
        }
        if (postFilteringThreshold < 0.0f || postFilteringThreshold > 1.0f) {
            throw new IllegalArgumentException("postFilteringThreshold must be between 0.0 and 1.0, got: " + postFilteringThreshold);
        }
        this.field = field;
        this.providedVisitRatio = visitRatio;
        this.k = k;
        this.filter = filter;
        this.numCands = numCands;
        this.postFilteringThreshold = postFilteringThreshold;
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
            && Objects.equals(providedVisitRatio, that.providedVisitRatio)
            && Objects.equals(postFilteringThreshold, that.postFilteringThreshold);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, k, filter, providedVisitRatio, postFilteringThreshold);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        vectorOpsCount = 0;
        IndexReader reader = indexSearcher.getIndexReader();

        final Weight filterWeight;
        if (filter != null) {
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                .build();
            Query rewritten = indexSearcher.rewrite(booleanQuery);
            if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            }
            filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else {
            filterWeight = null;
        }

        TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
        List<LeafReaderContext> leafReaderContexts = reader.leaves();
        List<VectorLeafSearchFilterMeta> leafSearchMetas = new ArrayList<>(leafReaderContexts.size());
        assert this instanceof IVFKnnFloatVectorQuery;
        int totalVectors = 0;
        for (LeafReaderContext leafReaderContext : leafReaderContexts) {
            LeafReader leafReader = leafReaderContext.reader();
            FloatVectorValues floatVectorValues = leafReader.getFloatVectorValues(field);
            if (floatVectorValues != null) {
                totalVectors += floatVectorValues.size();
                var liveDocs = leafReader.getLiveDocs();
                if (filterWeight == null) {
                    leafSearchMetas.add(
                        new VectorLeafSearchFilterMeta(
                            leafReaderContext,
                            liveDocs == null
                                ? ESAcceptDocs.ESAcceptDocsAll.INSTANCE
                                : new ESAcceptDocs.BitsAcceptDocs(liveDocs, leafReader.maxDoc())
                        )
                    );
                } else {
                    ScorerSupplier supplier = filterWeight.scorerSupplier(leafReaderContext);
                    if (supplier != null) {
                        var filterCost = Math.toIntExact(supplier.cost());
                        if (((float) filterCost / floatVectorValues.size()) >= 10000 * (1 + postFilteringThreshold)) {
                            leafSearchMetas.add(
                                new VectorLeafSearchFilterMeta(
                                    leafReaderContext,
                                    new ESAcceptDocs.PostFilterEsAcceptDocs(filterWeight, leafReaderContext, liveDocs)
                                )
                            );
                        } else {
                            leafSearchMetas.add(
                                new VectorLeafSearchFilterMeta(
                                    leafReaderContext,
                                    new ESAcceptDocs.ScorerSupplierAcceptDocs(supplier, liveDocs, leafReader.maxDoc())
                                )
                            );
                        }
                    }
                }
            }
        }

        // we request numCands as we are using it as an approximation measure
        // we need to ensure we are getting at least 2*k results to ensure we cover overspill duplicates
        // TODO move the logic for automatically adjusting percentages to the query, so we can only pass
        // 2k to the collector.
        int vectorsToCollect = Math.round(2f * k);
        IVFCollectorManager knnCollectorManager = getKnnCollectorManager(vectorsToCollect, indexSearcher);

        final float visitRatio;
        if (providedVisitRatio == 0.0f) {
            // dynamically set the percentage
            float expected = (float) Math.round(
                Math.log10(totalVectors) * Math.log10(totalVectors) * (Math.min(10_000, Math.max(numCands, 5 * k)))
            );
            visitRatio = expected / totalVectors;
        } else {
            visitRatio = providedVisitRatio;
        }

        List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
        for (VectorLeafSearchFilterMeta leafSearchMeta : leafSearchMetas) {
            tasks.add(
                () -> searchLeaf(leafSearchMeta.context, leafSearchMeta.filter, knnCollectorManager, visitRatio, postFilteringThreshold)
            );
        }
        TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

        // Merge sort the results
        TopDocs topK = TopDocs.merge(k, perLeafResults);
        vectorOpsCount = (int) topK.totalHits.value();
        if (topK.scoreDocs.length == 0) {
            return new MatchNoDocsQuery();
        }
        return new KnnScoreDocQuery(topK.scoreDocs, reader);
    }

    private TopDocs searchLeaf(
        LeafReaderContext context,
        AcceptDocs filterDocs,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        float postFilteringThreshold
    ) throws IOException {
        TopDocs results = approximateSearch(
            context,
            filterDocs,
            Integer.MAX_VALUE,
            knnCollectorManager,
            visitRatio,
            postFilteringThreshold
        );
        IntHashSet dedup = new IntHashSet(results.scoreDocs.length * 4 / 3);
        List<ScoreDoc> deduplicatedScoreDocs = new ArrayList<>(results.scoreDocs.length);
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (dedup.add(scoreDoc.doc)) {
                scoreDoc.doc += context.docBase;
                deduplicatedScoreDocs.add(scoreDoc);
            }
        }
        return new TopDocs(results.totalHits, deduplicatedScoreDocs.toArray(new ScoreDoc[0]));
    }

    abstract TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs filterDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        float postFilteringThreshold
    ) throws IOException;

    protected IVFCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        return new IVFCollectorManager(k, searcher);
    }

    @Override
    public final void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    static class IVFCollectorManager implements KnnCollectorManager {
        private final int k;
        final LongAccumulator longAccumulator;

        IVFCollectorManager(int k, IndexSearcher searcher) {
            this.k = k;
            longAccumulator = searcher.getIndexReader().leaves().size() > 1 ? new LongAccumulator(Long::max, LEAST_COMPETITIVE) : null;
        }

        @Override
        public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
            throws IOException {
            return new MaxScoreTopKnnCollector(k, visitedLimit, searchStrategy);
        }

        @Override
        public AbstractMaxScoreKnnCollector newOptimisticCollector(
            int visitedLimit,
            KnnSearchStrategy searchStrategy,
            LeafReaderContext context,
            int k
        ) throws IOException {
            return new MaxScoreTopKnnCollector(k, visitedLimit, searchStrategy);
        }
    }
}
