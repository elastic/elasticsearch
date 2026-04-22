/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

public class ESDiversifyingChildrenByteKnnVectorQuery extends DiversifyingChildrenByteKnnVectorQuery
    implements
        QueryProfilerProvider,
        PostFilterableKnnQuery {

    private final int kParam;
    private final int numCands;
    private long vectorOpsCount;
    private final boolean earlyTermination;
    private final BitSetProducer parentsFilter;
    private FixedBitSet seenDocs;
    private final int[] docsToSeed;

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy
    ) {
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, false, null, null);
    }

    public ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy,
        boolean earlyTermination
    ) {
        this(field, query, childFilter, k, numCands, parentsFilter, strategy, earlyTermination, null, null);
    }

    ESDiversifyingChildrenByteKnnVectorQuery(
        String field,
        byte[] query,
        Query childFilter,
        int k,
        int numCands,
        BitSetProducer parentsFilter,
        KnnSearchStrategy strategy,
        boolean earlyTermination,
        FixedBitSet seenDocs,
        int[] docsToSeed
    ) {
        super(field, query, childFilter, numCands, parentsFilter, strategy);
        this.kParam = k;
        this.numCands = numCands;
        this.earlyTermination = earlyTermination;
        this.parentsFilter = parentsFilter;
        this.seenDocs = seenDocs;
        this.docsToSeed = docsToSeed;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Weight filterWeight = createFilterWeight(indexSearcher);
        if (filter != null) {
            Query postFilterQuery = createPostFilterQuery(
                indexSearcher.getLeafContexts(),
                filterWeight,
                kParam,
                indexSearcher.getIndexReader(),
                null
            );
            if (postFilterQuery != null) {
                return postFilterQuery;
            }
        }
        return super.rewrite(indexSearcher);
    }

    private Weight createFilterWeight(IndexSearcher searcher) throws IOException {
        if (filter == null) {
            return null;
        }
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
        Query rewritten = searcher.rewrite(booleanQuery);
        if (rewritten.getClass() == MatchNoDocsQuery.class) {
            return null;
        }
        return searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
        TopDocs topK = TopDocs.merge(kParam, perLeafResults);
        vectorOpsCount = topK.totalHits.value();
        return topK;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(vectorOpsCount);
    }

    @Override
    public Query createInnerQuery(IndexReader reader, int[] docsVisited) {
        appendSeenDocs(seenDocs, docsVisited, reader.maxDoc());
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            new ExcludeDocsQuery(seenDocs, reader),
            kParam,
            numCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            seenDocs,
            docsVisited
        );
    }

    @Override
    public PostFilterableKnnQuery createPostFilterDelegate(float filterSelectivity) {
        int scaledK = (int) Math.max(NUM_CANDS_LIMIT, Math.ceil(kParam / filterSelectivity));
        int scaledNumCands = (int) Math.max(NUM_CANDS_LIMIT, Math.ceil((double) numCands / filterSelectivity));
        return new ESDiversifyingChildrenByteKnnVectorQuery(
            field,
            getTargetCopy(),
            null,
            scaledK,
            scaledNumCands,
            parentsFilter,
            searchStrategy,
            earlyTermination,
            null,
            null
        );
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            ByteVectorValues fvv = leaf.reader().getByteVectorValues(field);
            if (fvv != null) {
                totalVectors += fvv.size();
            }
        }
        return totalVectors;
    }

    // --- Accessors ---

    public KnnSearchStrategy getStrategy() {
        return searchStrategy;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
        var base = super.getKnnCollectorManager(k, searcher);
        if (docsToSeed != null && docsToSeed.length > 0) {
            base = new SeededRetryCollectorManager(base, docsToSeed, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
