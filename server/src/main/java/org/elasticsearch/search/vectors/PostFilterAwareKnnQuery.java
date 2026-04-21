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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with retry.
 * When post-filtering yields fewer than k results, retries with new delegates that avoid
 * re-visiting previously seen results (doc IDs for HNSW, centroid posting lists for IVF).
 * <p>
 * The retry loop runs internally (up to {@link #MAX_ROUNDS} rounds). Each round:
 * 1. Executes the delegate's search via rewrite
* 2. Applies the filter to raw results
 * 3. Accumulates filtered results
 * 4. If not enough results, creates a retry delegate and continues
 */
public class PostFilterAwareKnnQuery extends Query implements QueryProfilerProvider {

    public static final float POST_FILTERING_THRESHOLD = 0.7f;

    static final int MAX_ROUNDS = 5;

    private final PostFilterableKnnQuery delegate;
    private final Weight filterWeight;
    private final int k;
    private final IndexReader reader;
    private final LongConsumer vectorOpsCallback;
    private final BitSetProducer parentsFilter;
    private long totalVectorOps;

    public PostFilterAwareKnnQuery(
        PostFilterableKnnQuery delegate,
        Weight filterWeight,
        int k,
        IndexReader reader,
        LongConsumer vectorOpsCallback,
        BitSetProducer parentsFilter
    ) {
        this.delegate = delegate;
        this.filterWeight = filterWeight;
        this.k = k;
        this.reader = reader;
        this.vectorOpsCallback = vectorOpsCallback;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        ScoreDoc[] accumulated = new ScoreDoc[0];
        long vectorOps = 0;
        PostFilterableKnnQuery current = delegate;

        for (int round = 0; round < MAX_ROUNDS; round++) {
            ScoreDoc[] rawResults = current.findCandidates(searcher);
            vectorOps += current.vectorOpsCount();

            if (rawResults.length == 0) {
                break;
            }

            ScoreDoc[] filtered = applyFilter(rawResults, filterWeight, searcher);
            accumulated = mergeResults(accumulated, filtered);

            if (parentsFilter != null) {
                accumulated = deduplicateByParent(accumulated, searcher.getIndexReader(), parentsFilter);
            }

            if (accumulated.length >= k) {
                break;
            }
            current = current.createRetryQuery(searcher.getIndexReader(), rawResults);
        }

        // Propagate profiling info
        this.totalVectorOps = vectorOps;
        if (vectorOpsCallback != null) {
            vectorOpsCallback.accept(vectorOps);
        }

        if (accumulated.length == 0) {
            return Queries.NO_DOCS_INSTANCE;
        }
        int count = Math.min(k, accumulated.length);
        return new KnnScoreDocQuery(Arrays.copyOf(accumulated, count), reader);
    }

    /**
     * Applies the filter to ScoreDocs with global doc IDs. Groups docs by leaf for efficient
     * filter iterator advancement, then returns passing docs sorted by score descending.
     */
    static ScoreDoc[] applyFilter(ScoreDoc[] scoreDocs, Weight filterWeight, IndexSearcher searcher) throws IOException {
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

        // Group docs by leaf ordinal
        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<ScoreDoc>[] byLeaf = new List[leaves.size()];
        for (ScoreDoc sd : scoreDocs) {
            int leafOrd = ReaderUtil.subIndex(sd.doc, leaves);
            if (byLeaf[leafOrd] == null) {
                byLeaf[leafOrd] = new ArrayList<>();
            }
            byLeaf[leafOrd].add(sd);
        }

        List<ScoreDoc> passing = new ArrayList<>();
        for (int leafOrd = 0; leafOrd < leaves.size(); leafOrd++) {
            if (byLeaf[leafOrd] == null) continue;
            LeafReaderContext ctx = leaves.get(leafOrd);
            ScorerSupplier ss = filterWeight.scorerSupplier(ctx);
            if (ss == null) continue;

            DocIdSetIterator filterIter = ss.get(NO_MORE_DOCS).iterator();
            // Sort by local doc ID for efficient filter advancing
            List<ScoreDoc> leafDocs = byLeaf[leafOrd];
            leafDocs.sort(Comparator.comparingInt(sd -> sd.doc));

            int filterDoc = -1;
            for (ScoreDoc sd : leafDocs) {
                int localDoc = sd.doc - ctx.docBase;
                if (filterDoc < localDoc) {
                    filterDoc = filterIter.advance(localDoc);
                }
                if (filterDoc == localDoc) {
                    passing.add(sd);
                }
                if (filterDoc == NO_MORE_DOCS) break;
            }
        }

        // Sort by score descending
        passing.sort((a, b) -> Float.compare(b.score, a.score));
        return passing.toArray(new ScoreDoc[0]);
    }

    /**
     * Deduplicates results by parent document, keeping only the highest-scoring child per parent.
     * Input must be sorted by score descending (first child seen per parent wins).
     */
    static ScoreDoc[] deduplicateByParent(ScoreDoc[] docs, IndexReader reader, BitSetProducer parentsFilter) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        IntHashSet seenParents = new IntHashSet();
        List<ScoreDoc> deduped = new ArrayList<>();
        for (ScoreDoc sd : docs) {
            int leafOrd = ReaderUtil.subIndex(sd.doc, leaves);
            LeafReaderContext ctx = leaves.get(leafOrd);
            BitSet parentBitSet = parentsFilter.getBitSet(ctx);
            if (parentBitSet == null) continue;
            int localDoc = sd.doc - ctx.docBase;
            int parentDoc = parentBitSet.nextSetBit(localDoc);
            if (parentDoc == DocIdSetIterator.NO_MORE_DOCS) continue;
            int globalParent = parentDoc + ctx.docBase;
            if (seenParents.add(globalParent)) {
                deduped.add(sd);
            }
        }
        return deduped.toArray(new ScoreDoc[0]);
    }

    static ScoreDoc[] mergeResults(ScoreDoc[] existing, ScoreDoc[] newResults) {
        if (existing.length == 0) return newResults;
        if (newResults.length == 0) return existing;

        IntHashSet seen = new IntHashSet(existing.length + newResults.length);
        List<ScoreDoc> merged = new ArrayList<>(existing.length + newResults.length);
        int i = 0, j = 0;
        while (i < existing.length && j < newResults.length) {
            ScoreDoc next;
            if (existing[i].score >= newResults[j].score) {
                next = existing[i++];
            } else {
                next = newResults[j++];
            }
            if (seen.add(next.doc)) {
                merged.add(next);
            }
        }
        while (i < existing.length) {
            if (seen.add(existing[i].doc)) {
                merged.add(existing[i]);
            }
            i++;
        }
        while (j < newResults.length) {
            if (seen.add(newResults[j].doc)) {
                merged.add(newResults[j]);
            }
            j++;
        }
        return merged.toArray(new ScoreDoc[0]);
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        queryProfiler.addVectorOpsCount(totalVectorOps);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        throw new UnsupportedOperationException("PostFilterAwareKnnQuery should always be rewritten before createWeight");
    }

    @Override
    public String toString(String field) {
        return "PostFilterAwareKnnQuery[k=" + k + ", delegate=" + delegate + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        ((Query) delegate).visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostFilterAwareKnnQuery that = (PostFilterAwareKnnQuery) o;
        // Weight doesn't implement equals/hashCode, so use identity comparison.
        // This is acceptable because this query is always rewritten before execution
        // and never needs cache matching.
        return k == that.k
            && delegate.equals(that.delegate)
            && filterWeight == that.filterWeight
            && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), delegate, k, System.identityHashCode(filterWeight), parentsFilter);
    }

}
