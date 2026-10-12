/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.codec.vectors.cluster.BulkNeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFSegmentScan;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.IVFCollectorManager;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.LeafInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

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

    /** How one leaf is searched: as a whole (shapes that cannot be split) or in parts that share a scan. */
    record LeafSearch(LeafReaderContext ctx, IVFCollectorManager collectorManager, boolean usePrecondition) {}

    private final AbstractIVFKnnVectorQuery query;
    private final Weight filterWeight;
    private final float visitRatio;
    private final Leaf[] leaves;

    IVFScoringManager(AbstractIVFKnnVectorQuery query, Weight filterWeight, float visitRatio, List<LeafSearch> leafSearches)
        throws IOException {
        this.query = query;
        this.filterWeight = filterWeight;
        this.visitRatio = visitRatio;
        this.leaves = new Leaf[leafSearches.size()];
        for (int i = 0; i < leaves.length; i++) {
            long work = estimatedWork(leafSearches.get(i).ctx());
            leaves[i] = new Leaf(leafSearches.get(i), work, query.canSplitLeaves() && work > 0);
        }
    }

    /** Searches every leaf and returns one result per leaf, in leaf order, with SOAR duplicates removed. */
    TopDocs[] search(TaskExecutor executor) throws IOException {
        long[] work = new long[leaves.length];
        boolean[] splittable = new boolean[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            work[i] = leaves[i].work;
            splittable[i] = leaves[i].splittable;
        }
        int[][] slices = slices(work, splittable, MAX_WORK_PER_SLICE, MAX_PARTS_PER_LEAF, MAX_LEAVES_PER_SLICE);
        int[] parts = new int[leaves.length];
        for (int[] slice : slices) {
            for (int leaf : slice) {
                parts[leaf]++;
            }
        }
        for (int i = 0; i < leaves.length; i++) {
            leaves[i].scan = new IVFSegmentScan(Math.max(1, parts[i]));
        }
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int[] slice : slices) {
            tasks.add(() -> {
                for (int leaf : slice) {
                    search(leaves[leaf]);
                }
                return null;
            });
        }
        executor.invokeAll(tasks);
        TopDocs[] results = new TopDocs[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            results[i] = AbstractIVFKnnVectorQuery.keepBestCopies(leaves[i].result(), leaves[i].search.ctx().docBase);
        }
        return results;
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

    private void search(Leaf leaf) throws IOException {
        LeafSearch search = leaf.search;
        if (leaf.splittable == false) {
            leaf.setWhole(
                query.getLeafResults(search.ctx(), filterWeight, search.collectorManager(), visitRatio, search.usePrecondition())
            );
            return;
        }
        LeafInput input = leaf.input(query, filterWeight);
        if (input == null) {
            return; // the filter matches nothing in this leaf
        }
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, query.numCands, query.k, null, leaf.scan);
        AbstractMaxScoreKnnCollector collector = search.collectorManager().newCollector(Integer.MAX_VALUE, strategy, search.ctx());
        strategy.setCollector(collector);
        query.searchNearestVectors(search.ctx().reader(), input, collector);
        leaf.addPart(collector);
    }

    /** One leaf's state while the query runs. */
    private static final class Leaf {
        private final LeafSearch search;
        private final long work;
        private final boolean splittable;
        private IVFSegmentScan scan; // set before the leaf's parts run, once the parts are known
        private final List<AbstractMaxScoreKnnCollector> parts = new ArrayList<>();
        private LeafInput input;
        private boolean prepared;
        private TopDocs whole;

        Leaf(LeafSearch search, long work, boolean splittable) {
            this.search = search;
            this.work = work;
            this.splittable = splittable;
        }

        /** The leaf's query vector and accepted docs, prepared by its first part and shared with the others. */
        synchronized LeafInput input(AbstractIVFKnnVectorQuery query, Weight filterWeight) throws IOException {
            if (prepared == false) {
                input = query.leafInput(search.ctx(), filterWeight, search.usePrecondition());
                prepared = true;
            }
            return input;
        }

        synchronized void addPart(AbstractMaxScoreKnnCollector collector) {
            parts.add(collector);
        }

        synchronized void setWhole(TopDocs topDocs) {
            whole = topDocs;
        }

        /** The leaf's hits: the whole-leaf result, or the exact top hits of all parts with their total visited count. */
        synchronized TopDocs result() {
            if (whole != null || parts.isEmpty()) {
                return whole != null ? whole : AbstractIVFKnnVectorQuery.NO_RESULTS;
            }
            BulkNeighborQueue merged = BulkNeighborQueue.forMerging(parts.getFirst().k());
            long visited = 0;
            for (AbstractMaxScoreKnnCollector part : parts) {
                visited += part.visitedCount();
                // parts only exist for the plain queries, whose collectors are MaxScoreTopKnnCollector
                for (ScoreDoc hit : ((BulkKnnCollector) part).unsortedTopK().scoreDocs) {
                    merged.insertWithOverflowBulk(new int[] { hit.doc }, new float[] { hit.score }, 1, hit.score);
                }
            }
            ScoreDoc[] hits = new ScoreDoc[merged.size()];
            int[] at = { hits.length - 1 };
            merged.drain(encoded -> hits[at[0]--] = new ScoreDoc(merged.decodeNodeId(encoded), merged.decodeScore(encoded)));
            return new TopDocs(new TotalHits(visited, TotalHits.Relation.EQUAL_TO), hits);
        }
    }
}
