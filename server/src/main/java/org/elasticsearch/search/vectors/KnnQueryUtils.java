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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class KnnQueryUtils {

    public static float computeSelectivity(Weight filterWeight, List<LeafReaderContext> leaves, int totalVectors) throws IOException {
        long filterCost = 0;
        for (LeafReaderContext leafCtx : leaves) {
            ScorerSupplier ss = filterWeight.scorerSupplier(leafCtx);
            if (ss != null) {
                filterCost += ss.cost();
            }
        }
        return totalVectors > 0 ? Math.min(1f, (float) filterCost / totalVectors) : 0f;
    }

    /**
     * Combines a pre-filter with an {@link ExcludeDocsQuery} over already-collected docs into a
     * single filter for the augmented post-filter fallback. Returns {@code baseFilter} unchanged
     * when {@code excludedDocs} is empty, and the bare {@link ExcludeDocsQuery} when
     * {@code baseFilter} is null.
     */
    public static Query augmentFilter(Query baseFilter, int[] excludedDocs, IndexReader reader) {
        if (excludedDocs == null || excludedDocs.length == 0) {
            return baseFilter;
        }
        Query exclude = new ExcludeDocsQuery(excludedDocs, reader);
        if (baseFilter == null) {
            return exclude;
        }
        return new BooleanQuery.Builder().add(baseFilter, BooleanClause.Occur.FILTER).add(exclude, BooleanClause.Occur.FILTER).build();
    }

    public static Weight createFilterWeight(IndexSearcher searcher, Query filter, String field) throws IOException {
        if (filter == null) {
            return null;
        }
        var booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
        Query rewritten = searcher.rewrite(booleanQuery);
        if (rewritten.getClass() == MatchNoDocsQuery.class) {
            return null;
        }
        return searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    /**
     * Applies the filter to ScoreDocs with global doc IDs. Groups docs by leaf for efficient
     * filter iterator advancement, then returns passing docs sorted by score descending.
     */
    public static ScoreDoc[] applyFilter(ScoreDoc[] scoreDocs, Weight filterWeight, IndexSearcher searcher) throws IOException {
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

            DocIdSetIterator filterIter = ss.get(byLeaf[leafOrd].size()).iterator();
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
    public static ScoreDoc[] deduplicateByParent(ScoreDoc[] docs, IndexReader reader, BitSetProducer parentsFilter) throws IOException {
        if (parentsFilter == null) {
            return docs;
        }
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

    public static ScoreDoc[] mergeResults(ScoreDoc[] existing, ScoreDoc[] newResults) {
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

}
