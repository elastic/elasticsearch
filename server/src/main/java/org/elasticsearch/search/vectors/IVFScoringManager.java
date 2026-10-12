/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.cluster.BulkNeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFSegmentScan;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.IVFCollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.ToLongFunction;

/**
 * Runs one IVF query over a shard's leaves the way Lucene slices a search: small leaves are grouped into one task, and a
 * leaf with a lot of work is split into parts, separate tasks that share the leaf's {@link IVFSegmentScan}. Owns the
 * per-part collectors and reduces them to one {@link TopDocs} per leaf, in leaf order.
 */
final class IVFScoringManager {

    /** Posting entries a slice should scan; Lucene's equivalent is 250k docs. */
    static final long MAX_WORK_PER_SLICE = 8_192;
    /** Lucene splits a segment into at most 5 partitions. */
    static final int MAX_PARTS_PER_LEAF = 5;
    /** Lucene groups at most 5 segments into a slice. */
    static final int MAX_LEAVES_PER_SLICE = 5;
    /** With re-entry, the first round scans this fraction of each splittable leaf's budget. */
    static final float PAUSE_AT = 0.6f;
    /** ... and a leaf continues if a posting in this last fraction of its budget beat the shard bar. */
    static final float WINDOW = 0.1f;
    /** The window covers at least this many postings; small budgets would otherwise judge a leaf on one or two. */
    static final int MIN_WINDOW_POSTINGS = 10;

    /** How one leaf is searched: as a whole (shapes that cannot be split) or in parts that share a scan. */
    record LeafSearch(LeafReaderContext ctx, IVFCollectorManager collectorManager, boolean usePrecondition) {}

    private final AbstractIVFKnnVectorQuery query;
    private final Weight filterWeight;
    private final float visitRatio;
    private final int reentryMergeK;
    private final Leaf[] leaves;

    /**
     * @param reentryMergeK the size of the shard's rescore pool if leaves should re-enter optimistically (scan part of their
     *                      budget, then continue only if still producing competitive hits), or 0 to scan full budgets
     */
    IVFScoringManager(
        AbstractIVFKnnVectorQuery query,
        Weight filterWeight,
        float visitRatio,
        List<LeafSearch> leafSearches,
        int reentryMergeK
    ) throws IOException {
        this.query = query;
        this.filterWeight = filterWeight;
        this.visitRatio = visitRatio;
        this.reentryMergeK = filterWeight == null ? reentryMergeK : 0; // filtered searches always scan their full budget
        this.leaves = new Leaf[leafSearches.size()];
        for (int i = 0; i < leaves.length; i++) {
            LeafReaderContext ctx = leafSearches.get(i).ctx();
            long work = estimatedWork(ctx);
            leaves[i] = new Leaf(leafSearches.get(i), work, query.canSplitLeaves() && work > 0 && searchesPostings(ctx.reader()));
        }
    }

    /**
     * Searches every leaf and returns one result per leaf, in leaf order, with SOAR duplicates removed. With re-entry, like
     * Lucene's optimistic kNN collection, it runs two rounds: every leaf scans part of its budget, then only leaves that
     * are not tapped out continue. Decisions are taken between rounds, so they don't depend on thread timing.
     */
    TopDocs[] search(TaskExecutor executor) throws IOException {
        int[] all = new int[leaves.length];
        Arrays.setAll(all, i -> i);
        int[][] slices = slicesOf(all, leaf -> leaf.work);
        int[] parts = new int[leaves.length];
        for (int[] slice : slices) {
            for (int leaf : slice) {
                parts[leaf]++;
            }
        }
        for (int i = 0; i < leaves.length; i++) {
            leaves[i].scan = new IVFSegmentScan(Math.max(1, parts[i]));
            if (reentryMergeK > 0 && leaves[i].splittable) {
                leaves[i].scan.pauseAt(PAUSE_AT);
            }
        }
        run(executor, slices);
        if (reentryMergeK > 0) {
            run(executor, slicesOf(reenteringLeaves(shardBar()), leaf -> leaf.scan.remainingBudget()));
        }
        TopDocs[] results = new TopDocs[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            results[i] = AbstractIVFKnnVectorQuery.keepBestCopies(leaves[i].hits(), leaves[i].search.ctx().docBase);
        }
        return results;
    }

    private void run(TaskExecutor executor, int[][] slices) throws IOException {
        List<Callable<Void>> tasks = new ArrayList<>(slices.length);
        for (int[] slice : slices) {
            tasks.add(() -> {
                for (int leaf : slice) {
                    search(leaves[leaf]);
                }
                return null;
            });
        }
        executor.invokeAll(tasks);
    }

    /** Slices the given leaves by {@code work}; returns slices of leaf indices. */
    private int[][] slicesOf(int[] leafIndices, ToLongFunction<Leaf> work) {
        long[] w = new long[leafIndices.length];
        boolean[] splittable = new boolean[leafIndices.length];
        for (int i = 0; i < leafIndices.length; i++) {
            w[i] = work.applyAsLong(leaves[leafIndices[i]]);
            splittable[i] = leaves[leafIndices[i]].splittable;
        }
        int[][] slices = slices(w, splittable, MAX_WORK_PER_SLICE, MAX_PARTS_PER_LEAF, MAX_LEAVES_PER_SLICE);
        for (int[] slice : slices) {
            for (int j = 0; j < slice.length; j++) {
                slice[j] = leafIndices[slice[j]];
            }
        }
        return slices;
    }

    /** The lowest score a doc needs to enter the shard's rescore pool, given what the leaves found so far. */
    private float shardBar() {
        List<Float> scores = new ArrayList<>();
        for (Leaf leaf : leaves) {
            for (ScoreDoc hit : AbstractIVFKnnVectorQuery.keepBestCopies(leaf.hits(), 0).scoreDocs) {
                scores.add(hit.score);
            }
        }
        if (scores.size() < reentryMergeK) {
            return Float.NEGATIVE_INFINITY;
        }
        scores.sort(Comparator.reverseOrder());
        return scores.get(reentryMergeK - 1);
    }

    /** Resumes the paused leaves still producing hits above {@code bar}, stops the others, and returns those resumed. */
    private int[] reenteringLeaves(float bar) {
        List<Integer> resumed = new ArrayList<>();
        for (int i = 0; i < leaves.length; i++) {
            Leaf leaf = leaves[i];
            if (leaf.splittable == false || leaf.scan.remainingBudget() == 0) {
                continue; // searched whole, or already done
            }
            if (leaf.scan.worthContinuing(WINDOW, MIN_WINDOW_POSTINGS, bar, leaf.kLeaf)) {
                leaf.scan.resume();
                resumed.add(i);
            } else {
                leaf.scan.stop();
            }
        }
        return resumed.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Lucene's {@code IndexSearcher#slices} with posting entries instead of docs. Leaves are taken by decreasing work. A
     * splittable leaf whose work exceeds {@code maxWorkPerSlice} gets {@code ceil(work / maxWorkPerSlice)} slices of its own,
     * at most {@code maxPartsPerLeaf}; the other leaves are grouped until a slice exceeds {@code maxWorkPerSlice} or holds
     * {@code maxLeavesPerSlice} leaves. Returns the leaf indices of each slice; a split leaf appears in several.
     */
    static int[][] slices(long[] work, boolean[] splittable, long maxWorkPerSlice, int maxPartsPerLeaf, int maxLeavesPerSlice) {
        Integer[] order = new Integer[work.length];
        Arrays.setAll(order, i -> i);
        Arrays.sort(order, Comparator.comparingLong((Integer i) -> work[i]).reversed());
        List<int[]> slices = new ArrayList<>();
        List<Integer> group = new ArrayList<>();
        long groupWork = 0;
        for (int leaf : order) {
            if (splittable[leaf] && work[leaf] > maxWorkPerSlice) {
                int parts = (int) Math.min(maxPartsPerLeaf, (work[leaf] + maxWorkPerSlice - 1) / maxWorkPerSlice);
                for (int p = 0; p < parts; p++) {
                    slices.add(new int[] { leaf });
                }
                continue;
            }
            group.add(leaf);
            groupWork += work[leaf];
            if (group.size() >= maxLeavesPerSlice || groupWork > maxWorkPerSlice) {
                slices.add(group.stream().mapToInt(Integer::intValue).toArray());
                group.clear();
                groupWork = 0;
            }
        }
        if (group.isEmpty() == false) {
            slices.add(group.stream().mapToInt(Integer::intValue).toArray());
        }
        return slices.toArray(int[][]::new);
    }

    /** The posting entries the search will scan in this leaf; 0 if it has no vectors for the field. */
    private long estimatedWork(LeafReaderContext ctx) throws IOException {
        LeafReader reader = ctx.reader();
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(query.field);
        if (fieldInfo == null) {
            return 0;
        }
        KnnVectorValues values = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE
            ? reader.getByteVectorValues(query.field)
            : reader.getFloatVectorValues(query.field);
        int numVectors = values == null ? 0 : values.size();
        if (numVectors == 0) {
            return 0;
        }
        float ratio = visitRatio == 0f ? IVFVectorsReader.segmentVisitRatio(query.numCands, query.k, numVectors) : visitRatio;
        return IVFVectorsReader.visitBudget(ratio, numVectors);
    }

    /**
     * Whether the leaf's field is searched by DiskBBQ posting lists, the only search whose parts can share a scan. A field
     * can hold segments of other formats, e.g. after its index options changed from flat to bbq_disk.
     */
    private boolean searchesPostings(LeafReader reader) {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            return false;
        }
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            vectorsReader = perField.getFieldReader(query.field);
        }
        return vectorsReader instanceof IVFVectorsReader<?> ivf && ivf.searchesPostings(query.field);
    }

    private void search(Leaf leaf) throws IOException {
        LeafSearch search = leaf.search;
        if (leaf.splittable == false) {
            leaf.setWhole(
                query.getLeafResults(search.ctx(), filterWeight, search.collectorManager(), visitRatio, search.usePrecondition())
            );
            return;
        }
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, query.numCands, query.k, null, leaf.scan);
        AbstractMaxScoreKnnCollector collector = search.collectorManager().newCollector(Integer.MAX_VALUE, strategy, search.ctx());
        strategy.setCollector(collector);
        leaf.partSearch(query, filterWeight).search(collector);
        leaf.addPart(collector);
    }

    /** One leaf's state while the query runs. */
    private static final class Leaf {
        private final LeafSearch search;
        private final long work;
        private final boolean splittable;
        private IVFSegmentScan scan; // set before the leaf's parts run, once the parts are known
        private final List<AbstractMaxScoreKnnCollector> parts = new ArrayList<>();
        private int kLeaf;
        private AbstractIVFKnnVectorQuery.PartSearch partSearch;
        private TopDocs hits;

        Leaf(LeafSearch search, long work, boolean splittable) {
            this.search = search;
            this.work = work;
            this.splittable = splittable;
        }

        /** How to search one of the leaf's parts, prepared by its first part and shared with the others. */
        synchronized AbstractIVFKnnVectorQuery.PartSearch partSearch(AbstractIVFKnnVectorQuery query, Weight filterWeight)
            throws IOException {
            if (partSearch == null) {
                partSearch = query.partSearch(search.ctx(), filterWeight, search.usePrecondition());
            }
            return partSearch;
        }

        synchronized void addPart(AbstractMaxScoreKnnCollector collector) {
            parts.add(collector);
            kLeaf = collector.k();
        }

        synchronized void setWhole(TopDocs topDocs) {
            hits = topDocs;
        }

        /**
         * The leaf's hits so far: its whole-leaf result, or the exact top hits of all its finished parts (folding in parts
         * finished since the last call) with their total visited count.
         */
        synchronized TopDocs hits() {
            if (parts.isEmpty()) {
                return hits != null ? hits : AbstractIVFKnnVectorQuery.NO_RESULTS;
            }
            BulkNeighborQueue merged = BulkNeighborQueue.forMerging(kLeaf);
            long visited = 0;
            if (hits != null) {
                visited += hits.totalHits.value();
                insert(merged, hits);
            }
            for (AbstractMaxScoreKnnCollector part : parts) {
                visited += part.visitedCount();
                // parts only exist for the plain queries, whose collectors are MaxScoreTopKnnCollector
                insert(merged, ((BulkKnnCollector) part).unsortedTopK());
            }
            parts.clear();
            ScoreDoc[] sorted = new ScoreDoc[merged.size()];
            int[] at = { sorted.length - 1 };
            merged.drain(encoded -> sorted[at[0]--] = new ScoreDoc(merged.decodeNodeId(encoded), merged.decodeScore(encoded)));
            hits = new TopDocs(new TotalHits(visited, TotalHits.Relation.EQUAL_TO), sorted);
            return hits;
        }

        private static void insert(BulkNeighborQueue queue, TopDocs topDocs) {
            int count = topDocs.scoreDocs.length;
            int[] docs = new int[count];
            float[] scores = new float[count];
            float best = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < count; i++) {
                docs[i] = topDocs.scoreDocs[i].doc;
                scores[i] = topDocs.scoreDocs[i].score;
                best = Math.max(best, scores[i]);
            }
            if (count > 0) {
                queue.insertWithOverflowBulk(docs, scores, count, best);
            }
        }
    }
}
