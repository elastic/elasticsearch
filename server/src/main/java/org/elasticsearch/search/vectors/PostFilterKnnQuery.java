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
import org.apache.lucene.search.TopDocs;
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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A query that wraps a {@link PostFilterableKnnQuery} and applies post-filtering with retry.
 * When post-filtering yields fewer than k results, retries with new innerQueries that avoid
 * re-visiting previously seen results (doc IDs for HNSW, centroid posting lists for IVF).
 * <p>
 * The retry loop runs internally (up to {@link #MAX_ROUNDS} rounds). Each round:
 * 1. Executes the innerQuery's search via rewrite
 * 2. Applies the filter to raw results
 * 3. Accumulates filtered results
 * 4. If not enough results, creates a retry innerQuery and continues
 */
public class PostFilterKnnQuery extends Query implements QueryProfilerProvider {

    public static final float POST_FILTERING_THRESHOLD = 0.7f;

    static final int MAX_ROUNDS = 5;

    private final PostFilterableKnnQuery innerQuery;
    private final Weight filterWeight;
    private final int k;
    private long totalVectorOps;
    private final BitSetProducer parentsFilter;

    public PostFilterKnnQuery(
        PostFilterableKnnQuery innerQuery,
        Weight filterWeight,
        int k,
        IndexReader reader,
        BitSetProducer parentsFilter
    ) {
        this.innerQuery = innerQuery;
        this.filterWeight = filterWeight;
        this.k = k;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        ScoreDoc[] scoreDocs = new ScoreDoc[0];
        int[] seenDocs = new int[0];
        long vectorOps = 0;
        Query delegate = innerQuery.createInnerQuery(searcher.getIndexReader(), seenDocs);
        assert delegate instanceof PostFilterableKnnQuery;
        for (int round = 0; round < MAX_ROUNDS; round++) {
            TopDocs topDocs = searcher.search(delegate, Integer.MAX_VALUE);
            vectorOps += ((PostFilterableKnnQuery) delegate).vectorOpsCount();

            if (topDocs.scoreDocs.length == 0) {
                break;
            }

            // Accumulate this round's doc IDs into the running sorted array
            int[] roundDocs = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                roundDocs[i] = topDocs.scoreDocs[i].doc;
            }
            int[] merged = new int[seenDocs.length + roundDocs.length];
            System.arraycopy(seenDocs, 0, merged, 0, seenDocs.length);
            System.arraycopy(roundDocs, 0, merged, seenDocs.length, roundDocs.length);
            Arrays.sort(merged);
            seenDocs = merged;

            ScoreDoc[] filtered = applyFilter(topDocs.scoreDocs, filterWeight, searcher);
            scoreDocs = mergeResults(scoreDocs, filtered);

            if (parentsFilter != null) {
                scoreDocs = deduplicateByParent(scoreDocs, searcher.getIndexReader(), parentsFilter);
            }
            if (scoreDocs.length >= k) {
                break;
            }
            delegate = ((PostFilterableKnnQuery) delegate).createInnerQuery(searcher.getIndexReader(), seenDocs);
        }
        this.totalVectorOps = vectorOps;
        if (scoreDocs.length == 0) {
            return Queries.NO_DOCS_INSTANCE;
        }
        if (k < scoreDocs.length) {
            scoreDocs = Arrays.copyOf(scoreDocs, k);
        }
        return new KnnScoreDocQuery(scoreDocs, searcher.getIndexReader());
    }

    /**
     * Applies the filter to ScoreDocs with global doc IDs. Groups docs by leaf for efficient
     * filter iterator advancement, then returns passing docs sorted by score descending.
     */
    static ScoreDoc[] applyFilter(ScoreDoc[] scoreDocs, Weight filterWeight, IndexSearcher searcher) throws IOException {
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

        // group docs by leaf ordinal
        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<ScoreDoc>[] byLeaf = new List[leaves.size()];
        for (ScoreDoc sd : scoreDocs) {
            int leafOrd = ReaderUtil.subIndex(sd.doc, leaves);
            if (byLeaf[leafOrd] == null) {
                byLeaf[leafOrd] = new ArrayList<>();
            }
            byLeaf[leafOrd].add(sd);
        }

        // filter each leaf separately
        List<ScoreDoc> passingDocs = new ArrayList<>();
        for (int leafOrd = 0; leafOrd < leaves.size(); leafOrd++) {
            if (byLeaf[leafOrd] == null) continue;
            LeafReaderContext ctx = leaves.get(leafOrd);
            ScorerSupplier ss = filterWeight.scorerSupplier(ctx);
            if (ss == null) continue;

            DocIdSetIterator filterIter = ss.get(NO_MORE_DOCS).iterator();
            List<ScoreDoc> leafDocs = byLeaf[leafOrd];
            leafDocs.sort(Comparator.comparingInt(sd -> sd.doc));

            int filterDoc = -1;
            for (ScoreDoc sd : leafDocs) {
                int localDoc = sd.doc - ctx.docBase;
                if (filterDoc < localDoc) {
                    filterDoc = filterIter.advance(localDoc);
                }
                if (filterDoc == localDoc) {
                    passingDocs.add(sd);
                }
                if (filterDoc == NO_MORE_DOCS) break;
            }
        }

        // srot back by score descending
        passingDocs.sort((a, b) -> Float.compare(b.score, a.score));
        return passingDocs.toArray(new ScoreDoc[0]);
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

    public long getTotalVectorOps() {
        return totalVectorOps;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        throw new UnsupportedOperationException("PostFilterKnnQuery does not support [createWeight]. Missing a rewrite?");
    }

    @Override
    public String toString(String field) {
        return "PostFilterKnnQuery[k=" + k + ", innerQuery=" + innerQuery + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        ((Query) innerQuery).visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostFilterKnnQuery that = (PostFilterKnnQuery) o;
        // Weight doesn't implement equals/hashCode, so use identity comparison.
        // This is acceptable because this query is always rewritten before execution
        // and never needs cache matching.
        return k == that.k
            && innerQuery.equals(that.innerQuery)
            && filterWeight == that.filterWeight
            && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), innerQuery, k, System.identityHashCode(filterWeight), parentsFilter);
    }

}
