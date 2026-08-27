/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class KnnQueryUtils {

    public record FilterWeight(@Nullable Weight weight) {
        static final FilterWeight MATCH_NO_DOCS = new FilterWeight(null);
    }

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

    public static FilterWeight createFilterWeight(IndexSearcher searcher, Query filter, String field) throws IOException {
        if (filter == null) {
            return null;
        }
        var booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
        Query rewritten = searcher.rewrite(booleanQuery);
        if (rewritten == MatchNoDocsQuery.INSTANCE) {
            return FilterWeight.MATCH_NO_DOCS;
        }
        return new FilterWeight(searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f));
    }

    /**
     * Deduplicates {@code docs} and returns the top {@code k} hits sorted by score descending.
     * When {@code parentsFilter} is non-null, dedup is by parent doc (highest-scoring child per
     * parent wins); otherwise it is by global doc id (highest-scoring duplicate wins). Top-k is
     * found via {@link ArrayUtil#select} (introselect), so only the chosen k are sorted at the end
     * rather than the full deduplicated array.
     *
     * <p>Input order is irrelevant — both the dedup pass and the partition pass treat candidates
     * symmetrically.
     */
    public static ScoreDoc[] dedupAndSelectTopK(ScoreDoc[] docs, IndexReader reader, BitSetProducer parentsFilter, int k)
        throws IOException {
        if (docs.length == 0 || k == 0) {
            return new ScoreDoc[0];
        }
        ScoreDoc[] deduped = parentsFilter != null ? dedupByParent(docs, reader, parentsFilter) : dedupByDocId(docs);
        return selectTopK(deduped, k);
    }

    private static ScoreDoc[] dedupByParent(ScoreDoc[] docs, IndexReader reader, BitSetProducer parentsFilter) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        BitSet[] bitSetByLeaf = new BitSet[leaves.size()];
        boolean[] resolved = new boolean[leaves.size()];
        IntObjectHashMap<ScoreDoc> bestByParent = new IntObjectHashMap<>(docs.length);
        for (ScoreDoc sd : docs) {
            int leafOrd = ReaderUtil.subIndex(sd.doc, leaves);
            LeafReaderContext ctx = leaves.get(leafOrd);
            if (resolved[leafOrd] == false) {
                bitSetByLeaf[leafOrd] = parentsFilter.getBitSet(ctx);
                resolved[leafOrd] = true;
            }
            BitSet parentBitSet = bitSetByLeaf[leafOrd];
            if (parentBitSet == null) continue;
            int localDoc = sd.doc - ctx.docBase;
            int parentDoc = parentBitSet.nextSetBit(localDoc);
            if (parentDoc == DocIdSetIterator.NO_MORE_DOCS) continue;
            int globalParent = parentDoc + ctx.docBase;
            ScoreDoc existing = bestByParent.get(globalParent);
            if (existing == null || sd.score > existing.score) {
                bestByParent.put(globalParent, sd);
            }
        }
        return toArray(bestByParent);
    }

    private static ScoreDoc[] dedupByDocId(ScoreDoc[] docs) {
        IntObjectHashMap<ScoreDoc> bestByDoc = new IntObjectHashMap<>(docs.length);
        for (ScoreDoc sd : docs) {
            ScoreDoc existing = bestByDoc.get(sd.doc);
            if (existing == null || sd.score > existing.score) {
                bestByDoc.put(sd.doc, sd);
            }
        }
        return toArray(bestByDoc);
    }

    private static ScoreDoc[] toArray(IntObjectHashMap<ScoreDoc> map) {
        ScoreDoc[] out = new ScoreDoc[map.size()];
        int i = 0;
        for (IntObjectHashMap.IntObjectCursor<ScoreDoc> cursor : map) {
            out[i++] = cursor.value;
        }
        return out;
    }

    private static ScoreDoc[] selectTopK(ScoreDoc[] docs, int k) {
        Comparator<ScoreDoc> byScoreDesc = (a, b) -> Float.compare(b.score, a.score);
        if (docs.length <= k) {
            ArrayUtil.introSort(docs, byScoreDesc);
            return docs;
        }
        // Partition around the kth-best so docs[0..k) holds the k highest-scoring entries
        // (in unspecified order), then sort just those k in place.
        ArrayUtil.select(docs, 0, docs.length, k - 1, byScoreDesc);
        ArrayUtil.introSort(docs, 0, k, byScoreDesc);
        return Arrays.copyOf(docs, k);
    }

    /**
     * Merges two disjoint, individually-sorted int arrays into one sorted array
     */
    public static int[] sortedMerge(int[] a, int[] b) {
        if (a.length == 0) return b.length == 0 ? a : b.clone();
        if (b.length == 0) return a.clone();
        int[] merged = new int[a.length + b.length];
        int i = 0, j = 0, k = 0;
        while (i < a.length && j < b.length) {
            merged[k++] = a[i] <= b[j] ? a[i++] : b[j++];
        }
        if (i < a.length) {
            System.arraycopy(a, i, merged, k, a.length - i);
        } else {
            System.arraycopy(b, j, merged, k, b.length - j);
        }
        return merged;
    }

    public static ScoreDoc[] mergeScoreDocArrays(ScoreDoc[] left, ScoreDoc[] right) {
        if (left.length == 0) return right.length == 0 ? left : Arrays.copyOf(right, right.length);
        if (right.length == 0) return Arrays.copyOf(left, left.length);
        ScoreDoc[] out = new ScoreDoc[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

}
