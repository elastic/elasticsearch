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
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.IVFCollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

/**
 * Runs one IVF query over a shard's leaves the way Elasticsearch slices a search, in slices of about equal work: small leaves
 * are grouped into one task, and a leaf with a lot of work is split into parts, separate tasks that share the leaf's
 * {@link IVFSegmentScan}. Owns the per-part collectors and reduces them to one {@link TopDocs} per leaf, in leaf order.
 */
final class IVFScoringManager {

    /**
     * No more slices than cores: more cannot run at once and only add per-slice work. The search pool has more threads than
     * that (1.5 per core), so its size alone is not enough.
     */
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

    /** How one leaf is searched: as a whole (shapes that cannot be split) or in parts that share a scan. */
    record LeafSearch(LeafReaderContext ctx, IVFCollectorManager collectorManager, boolean usePrecondition) {}

    private final AbstractIVFKnnVectorQuery query;
    private final Weight filterWeight;
    private final float visitRatio;
    private final int parallelism;
    private final Leaf[] leaves;

    /**
     * @param maxParallelSlices how many slices the searcher runs in parallel (1 without an executor): the query is cut into
     *                          at most that many slices, and no more than there are cores
     */
    IVFScoringManager(
        AbstractIVFKnnVectorQuery query,
        Weight filterWeight,
        float visitRatio,
        List<LeafSearch> leafSearches,
        int maxParallelSlices
    ) throws IOException {
        this.query = query;
        this.filterWeight = filterWeight;
        this.visitRatio = visitRatio;
        this.parallelism = Math.min(maxParallelSlices, PROCESSORS);
        this.leaves = new Leaf[leafSearches.size()];
        for (int i = 0; i < leaves.length; i++) {
            LeafReaderContext ctx = leafSearches.get(i).ctx();
            long work = estimatedWork(ctx);
            leaves[i] = new Leaf(leafSearches.get(i), work, query.canSplitLeaves() && work > 0 && searchesPostings(ctx.reader()));
        }
    }

    /**
     * Searches every leaf and returns one result per leaf, in leaf order, with SOAR duplicates removed. A leaf split into
     * parts is opened first (planned, and its budget listed), so that its parts start with work instead of waiting for the
     * plan.
     */
    TopDocs[] search(TaskExecutor executor) throws IOException {
        long[] work = new long[leaves.length];
        boolean[] splittable = new boolean[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            work[i] = leaves[i].work;
            splittable[i] = leaves[i].splittable;
        }
        int[][] slices = slices(work, splittable, parallelism);
        int[] parts = new int[leaves.length];
        for (int[] slice : slices) {
            for (int leaf : slice) {
                parts[leaf]++;
            }
        }
        List<int[]> split = new ArrayList<>();
        for (int i = 0; i < leaves.length; i++) {
            leaves[i].scan = new IVFSegmentScan(Math.max(1, parts[i]));
            if (parts[i] > 1) {
                leaves[i].scan.pause(); // opening claims nothing
                split.add(new int[] { i });
            }
        }
        run(executor, split.toArray(int[][]::new));
        for (Leaf leaf : leaves) {
            leaf.scan.resume();
        }
        run(executor, slices);
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

    /**
     * Elasticsearch's slicing rule ({@code ContextIndexSearcher#computeSlices}) with posting entries instead of docs, plus
     * Lucene's partitions of big leaves: aim for {@code parallelism} slices of equal work, none with less than
     * {@link ContextIndexSearcher#MINIMUM_DOCS_PERCENT_PER_SLICE} of the total. A leaf with at least a slice's work gets its
     * share of the slices, rounded, as parts of its own (one if it cannot be split). The other leaves are grouped, by
     * decreasing work, until a group has a slice's work; what is left over makes a slice of its own if it has the minimum
     * share and a slice is free, else joins the least loaded slices. Returns the leaf indices of each slice; a split leaf
     * appears in several.
     */
    static int[][] slices(long[] work, boolean[] splittable, int parallelism) {
        double minShare = ContextIndexSearcher.MINIMUM_DOCS_PERCENT_PER_SLICE;
        int maxSlices = (int) Math.min(parallelism, Math.round(1 / minShare));
        long total = Arrays.stream(work).sum();
        if (total == 0 || maxSlices == 1) {
            return new int[][] { IntStream.range(0, work.length).toArray() };
        }
        double sliceWork = (double) total / maxSlices;
        Integer[] order = new Integer[work.length];
        Arrays.setAll(order, i -> i);
        Arrays.sort(order, Comparator.comparingLong((Integer i) -> work[i]).reversed());
        List<List<Integer>> slices = new ArrayList<>();
        List<Long> loads = new ArrayList<>();
        List<Integer> rest = new ArrayList<>();
        for (int leaf : order) {
            long parts = work[leaf] < sliceWork
                ? 0
                : Math.min(Math.round(work[leaf] / sliceWork), (long) (work[leaf] / (minShare * total)));
            parts = Math.min(splittable[leaf] ? parts : Math.min(parts, 1), maxSlices - slices.size());
            if (parts == 0) {
                rest.add(leaf);
            }
            for (long p = 0; p < parts; p++) {
                slices.add(new ArrayList<>(List.of(leaf)));
                loads.add(work[leaf] / parts);
            }
        }
        List<Integer> group = new ArrayList<>();
        long groupWork = 0;
        for (int leaf : rest) {
            group.add(leaf);
            groupWork += work[leaf];
            if (groupWork > sliceWork && slices.size() < maxSlices) {
                slices.add(group);
                loads.add(groupWork);
                group = new ArrayList<>();
                groupWork = 0;
            }
        }
        if (slices.isEmpty() || (groupWork >= minShare * total && slices.size() < maxSlices)) {
            slices.add(group);
        } else {
            for (int leaf : group) {
                int least = loads.indexOf(Collections.min(loads));
                slices.get(least).add(0, leaf); // ahead of the part, which then claims what the other parts left
                loads.set(least, loads.get(least) + work[leaf]);
            }
        }
        return slices.stream()
            .filter(slice -> slice.isEmpty() == false)
            .map(slice -> slice.stream().mapToInt(i -> i).toArray())
            .toArray(int[][]::new);
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

        /** The leaf's hits: its whole-leaf result, or the exact top hits of all its parts with their total visited count. */
        synchronized TopDocs hits() {
            if (parts.isEmpty()) {
                return hits != null ? hits : AbstractIVFKnnVectorQuery.NO_RESULTS;
            }
            BulkNeighborQueue merged = BulkNeighborQueue.forMerging(kLeaf);
            long visited = 0;
            for (AbstractMaxScoreKnnCollector part : parts) {
                visited += part.visitedCount();
                // parts only exist for the plain queries, whose collectors are MaxScoreTopKnnCollector
                insert(merged, ((BulkKnnCollector) part).unsortedTopK());
            }
            ScoreDoc[] sorted = new ScoreDoc[merged.size()];
            int[] at = { sorted.length - 1 };
            merged.drain(encoded -> sorted[at[0]--] = new ScoreDoc(merged.decodeNodeId(encoded), merged.decodeScore(encoded)));
            return new TopDocs(new TotalHits(visited, TotalHits.Relation.EQUAL_TO), sorted);
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
