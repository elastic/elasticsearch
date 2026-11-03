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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
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
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeSet;

public final class KnnQueryUtils {
    private static final int MAX_SAMPLES = 500;

    public record FilterWeight(@Nullable Weight weight) {
        static final FilterWeight MATCH_NO_DOCS = new FilterWeight(null);
    }

    /**
     * Total number of {@code FLOAT32}-encoded vectors indexed for {@code field} across {@code leaves}.
     * Use for {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType#FLOAT} and
     * {@code BFLOAT16} fields, which both index as {@link org.apache.lucene.index.VectorEncoding#FLOAT32}.
     * A leaf that does not index {@code field} contributes nothing.
     */
    public static int countFloatVectors(String field, List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            FloatVectorValues values = leaf.reader().getFloatVectorValues(field);
            if (values != null) {
                totalVectors += values.size();
            }
        }
        return totalVectors;
    }

    /**
     * Total number of {@code BYTE}-encoded vectors indexed for {@code field} across {@code leaves}.
     * Counting the wrong encoding yields 0, which {@link #computeSelectivity} turns into a selectivity of
     * 0 and silently disables post-filtering — so implementations must pick the counter that matches the
     * query's own encoding.
     */
    public static int countByteVectors(String field, List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            ByteVectorValues values = leaf.reader().getByteVectorValues(field);
            if (values != null) {
                totalVectors += values.size();
            }
        }
        return totalVectors;
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
     * Deduplicates {@code docs} by global doc id (highest-scoring duplicate wins) and returns the top
     * {@code k} hits sorted by score descending. Top-k is found via {@link ArrayUtil#select} (introselect),
     * so only the chosen k are sorted at the end rather than the full deduplicated array.

     */
    public static ScoreDoc[] dedupAndSelectTopK(ScoreDoc[] docs, int k) {
        if (docs.length == 0 || k == 0) {
            return new ScoreDoc[0];
        }
        return selectTopK(dedupByDocId(docs), k);
    }

    /**
     * Expands each matched child into every child doc ID in its parent's block.
     * Used to build the post-filter retry's exclusion set for nested (block-join) queries: once a
     * parent has produced a matching hit we want no more children from it (we keep only one hit per
     * parent), so the retry should skip the whole block rather than just the one matched child. A
     * parent whose child was <em>filtered out</em> instead is deliberately not expanded here - it stays
     * eligible, because a deeper, untried child of it may still pass the filter.
     */
    static int[] expandToParentBlocks(ScoreDoc[][] matchingPerLeaf, IndexReader reader, BitSetProducer parentsFilter) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        int[] parentBlockDocIdsToFilter = new int[16];
        int size = 0;
        for (int leafOrd = 0; leafOrd < matchingPerLeaf.length; leafOrd++) {
            ScoreDoc[] cands = matchingPerLeaf[leafOrd];
            if (cands == null || cands.length == 0) continue;
            LeafReaderContext ctx = leaves.get(leafOrd);
            BitSet parentBitSet = parentsFilter.getBitSet(ctx);
            if (parentBitSet == null) continue;
            int docBase = ctx.docBase;
            int lastParentLocal = -1;
            for (ScoreDoc sd : cands) {
                int localDoc = sd.doc - docBase;
                int parentLocal = parentBitSet.nextSetBit(localDoc);
                // Skip if the child has no parent (should not happen for a real candidate) or if this
                // parent's block was already emitted for the previous, lower-ordered candidate.
                if (parentLocal == DocIdSetIterator.NO_MORE_DOCS || parentLocal == lastParentLocal) {
                    continue;
                }
                lastParentLocal = parentLocal;
                int blockStartLocal = parentLocal == 0 ? 0 : parentBitSet.prevSetBit(parentLocal - 1) + 1;
                int blockLen = parentLocal - blockStartLocal + 1;
                parentBlockDocIdsToFilter = ArrayUtil.grow(parentBlockDocIdsToFilter, size + blockLen);
                for (int local = blockStartLocal; local <= parentLocal; local++) {
                    parentBlockDocIdsToFilter[size++] = local + docBase;
                }
            }
        }
        return size == parentBlockDocIdsToFilter.length
            ? parentBlockDocIdsToFilter
            : ArrayUtil.copyOfSubArray(parentBlockDocIdsToFilter, 0, size);
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

    /**
     * A half-open range of vector ordinals in one leaf. Callers supplying multiple ranges must avoid overlapping ordinals
     * within a leaf, so sliced queries can sample only their searchable vector population.
     */
    record VectorRange(LeafReaderContext leaf, KnnVectorValues vectors, int fromOrd, int toOrd) {
        VectorRange {
            if (fromOrd < 0 || toOrd < fromOrd || toOrd > vectors.size()) {
                throw new IllegalArgumentException("invalid vector ordinal range");
            }
        }

        int size() {
            return toOrd - fromOrd;
        }
    }

    /** Estimates filter selectivity among the indexed vectors of {@code field} across all leaves. */
    static float estimateFilterSelectivityBySampling(Weight filterWeight, List<LeafReaderContext> leaves, String field) throws IOException {
        List<KnnQueryUtils.VectorRange> ranges = new ArrayList<>();
        for (LeafReaderContext leaf : leaves) {
            FieldInfo info = leaf.reader().getFieldInfos().fieldInfo(field);
            if (info == null || info.getVectorDimension() == 0) {
                continue;
            }
            KnnVectorValues vectors = switch (info.getVectorEncoding()) {
                case BYTE -> leaf.reader().getByteVectorValues(field);
                case FLOAT32 -> leaf.reader().getFloatVectorValues(field);
            };
            if (vectors != null && vectors.size() > 0) {
                ranges.add(new KnnQueryUtils.VectorRange(leaf, vectors, 0, vectors.size()));
            }
        }
        return estimateFilterSelectivityBySampling(filterWeight, ranges);
    }

    /** Uses a finite-population adjustment and caps the sample count at the population size and {@code MAX_SAMPLES}. */
    static int sampleSize(long totalVectors) {
        if (totalVectors <= 0) {
            return 0;
        }
        // Evaluate N * B / (N + B - 1) in floating point to avoid long overflow.
        double size = MAX_SAMPLES / (1.0 + (MAX_SAMPLES - 1.0) / totalVectors);
        return (int) Math.min(Math.min(totalVectors, MAX_SAMPLES), Math.ceil(size));
    }

    /**
     * Samples ordinal positions uniformly without replacement across the combined vector population.
     * The fixed seed gives the same ordinals for the same total vector count. The sampled documents also remain the same
     * when the ranges and reader are unchanged, including across query-cache warming.
     */
    static long[] sampleOrdinals(long totalVectors) {
        SplittableRandom random = new SplittableRandom(0x746172676574L);
        TreeSet<Long> ordinals = new TreeSet<>();
        // Floyd's algorithm: bounded work and storage even for very large vector populations.
        for (long i = totalVectors - sampleSize(totalVectors); i < totalVectors; i++) {
            if (ordinals.add(random.nextLong(i + 1)) == false) {
                ordinals.add(i);
            }
        }
        return ordinals.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Estimates the fraction of sampled live vector documents that match the filter. Deleted samples are excluded from
     * both numerator and denominator, without replenishment or a full live-vector scan. Returns zero when no live samples
     * remain. The sample budget bounds membership checks, although creating a filter scorer may materialize query results.
     */
    static float estimateFilterSelectivityBySampling(Weight filterWeight, List<KnnQueryUtils.VectorRange> ranges) throws IOException {
        long totalVectors = ranges.stream().mapToLong(KnnQueryUtils.VectorRange::size).sum();
        if (totalVectors == 0) {
            return 0f;
        }
        Map<LeafReaderContext, IntArrayList> perLeafDocs = new LinkedHashMap<>();
        int rangeIndex = 0;
        long rangeStart = 0;
        for (long ordinal : sampleOrdinals(totalVectors)) {
            KnnQueryUtils.VectorRange range = ranges.get(rangeIndex);
            while (ordinal >= rangeStart + range.size()) {
                rangeStart += range.size();
                range = ranges.get(++rangeIndex);
            }
            int doc = range.vectors().ordToDoc(range.fromOrd() + Math.toIntExact(ordinal - rangeStart));
            Bits liveDocs = range.leaf().reader().getLiveDocs();
            if (liveDocs == null || liveDocs.get(doc)) {
                perLeafDocs.computeIfAbsent(range.leaf(), ignored -> new IntArrayList()).add(doc);
            }
        }
        int sampled = 0;
        int matched = 0;
        for (Map.Entry<LeafReaderContext, IntArrayList> entry : perLeafDocs.entrySet()) {
            LeafReaderContext leaf = entry.getKey();
            int[] docs = entry.getValue().sort().toArray();
            Bits matches = Lucene.asSequentialAccessBits(leaf.reader().maxDoc(), filterWeight.scorerSupplier(leaf), docs.length);
            sampled += docs.length;
            for (int doc : docs) {
                if (matches.get(doc)) {
                    matched++;
                }
            }
        }
        return sampled == 0 ? 0f : (float) matched / sampled;
    }
}
