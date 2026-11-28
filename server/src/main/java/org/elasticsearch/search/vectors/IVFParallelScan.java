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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFSegmentScan;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.LeafInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Searches a shard's leaves for one IVF query, one task per leaf, except that a leaf with a large share of the vectors the
 * query is expected to score is split into parts: several tasks that share the leaf's {@link IVFSegmentScan}, and together
 * visit what one task would.
 */
final class IVFParallelScan {

    /** The fewest vectors a part may be expected to score: in benchmarks, parts with fewer made a query no faster. */
    static final long MIN_VECTORS_TO_SCORE_PER_PART = 10_000;

    /**
     * The least share of the shard's scoring a part may have, in percent: the share Elasticsearch asks of a slice
     * ({@link ContextIndexSearcher#MINIMUM_DOCS_PERCENT_PER_SLICE}).
     */
    static final int MIN_SCORING_PCT_PER_PART = (int) Math.round(100 * ContextIndexSearcher.MINIMUM_DOCS_PERCENT_PER_SLICE);

    /** A leaf with fewer vectors is never split: its scan is too short for a split to pay. Not final, so tests can lower it. */
    static int minVectorsToSplit = 100_000;

    private final AbstractIVFKnnVectorQuery query;
    private final Weight filterWeight;
    private final float visitRatio;
    private final Leaf[] leaves;

    IVFParallelScan(
        AbstractIVFKnnVectorQuery query,
        Weight filterWeight,
        float visitRatio,
        List<LeafInput> leafInputs,
        int maxParallelTasks
    ) throws IOException {
        this.query = query;
        this.filterWeight = filterWeight;
        this.visitRatio = visitRatio;
        long[] estimatedVectorsToScore = new long[leafInputs.size()];
        boolean[] splittable = new boolean[leafInputs.size()];
        for (int i = 0; i < leafInputs.size(); i++) {
            LeafReaderContext ctx = leafInputs.get(i).ctx();
            int vectors = searchedVectors(ctx);
            estimatedVectorsToScore[i] = estimateVectorsToScore(vectors); // what the parts of a leaf can share
            splittable[i] = vectors >= minVectorsToSplit && scansPostingLists(ctx.reader());
        }
        int[] partsPerLeaf = partsPerLeaf(estimatedVectorsToScore, splittable, maxParallelTasks);
        this.leaves = new Leaf[leafInputs.size()];
        for (int i = 0; i < leaves.length; i++) {
            leaves[i] = new Leaf(leafInputs.get(i), estimatedVectorsToScore[i], partsPerLeaf[i]);
        }
    }

    /** Whether any leaf is split. If none is, the caller searches as without a parallel scan. */
    boolean splitsALeaf() {
        for (Leaf leaf : leaves) {
            if (leaf.scan != null) {
                return true;
            }
        }
        return false;
    }

    /** One result per leaf, in leaf order, with shard-wide doc ids and each doc at most once. */
    TopDocs[] search(TaskExecutor executor) throws IOException {
        prepareSplitLeaves(executor);
        executor.invokeAll(tasks());
        TopDocs[] results = new TopDocs[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            results[i] = leaves[i].hits();
        }
        return results;
    }

    /** Plans each split leaf before its parts start, so that they find its posting lists ready to take. */
    private void prepareSplitLeaves(TaskExecutor executor) throws IOException {
        List<Callable<Void>> preparations = new ArrayList<>();
        for (Leaf leaf : leaves) {
            if (leaf.scan != null) {
                preparations.add(() -> {
                    prepareSplitLeaf(leaf);
                    return null;
                });
            }
        }
        executor.invokeAll(preparations);
    }

    /** Works out how to search a part of the leaf, then searches it as a part would while its scan prepares: nothing is visited. */
    private void prepareSplitLeaf(Leaf leaf) throws IOException {
        leaf.partSearch = query.partSearch(leaf.input.ctx(), filterWeight, leaf.input.usePrecondition());
        leaf.scan.prepare(() -> searchPart(leaf));
    }

    /** One task per part of a split leaf and one per other leaf, the biggest leaves first. */
    private List<Callable<Void>> tasks() {
        Leaf[] biggestFirst = leaves.clone();
        Arrays.sort(biggestFirst, Comparator.comparingLong((Leaf leaf) -> leaf.estimatedVectorsToScore).reversed());
        List<Callable<Void>> tasks = new ArrayList<>();
        for (Leaf leaf : biggestFirst) {
            if (leaf.input.collectorManager() == null) {
                continue; // the leaf has no vectors for the field
            }
            if (leaf.scan == null) {
                tasks.add(() -> {
                    leaf.wholeLeafHits = query.searchLeaf(leaf.input, filterWeight, visitRatio);
                    return null;
                });
            } else {
                for (int part = 0; part < leaf.parts; part++) {
                    tasks.add(() -> {
                        searchPart(leaf);
                        return null;
                    });
                }
            }
        }
        return tasks;
    }

    /**
     * Parts per leaf; 1 means searched whole. A leaf expected to score at least one task's worth of vectors gets its share
     * of the tasks, rounded. No part is expected to score fewer than {@link #MIN_VECTORS_TO_SCORE_PER_PART} vectors, or
     * less than {@link #MIN_SCORING_PCT_PER_PART} percent of the shard's.
     */
    static int[] partsPerLeaf(long[] estimatedVectorsToScore, boolean[] splittable, int maxParallelTasks) {
        int maxParts = Math.min(maxParallelTasks, 100 / MIN_SCORING_PCT_PER_PART);
        long estimatedTotalVectors = Arrays.stream(estimatedVectorsToScore).sum();
        int[] parts = new int[estimatedVectorsToScore.length];
        Arrays.fill(parts, 1);
        if (estimatedTotalVectors == 0 || maxParts == 1) {
            return parts;
        }
        double vectorsPerTask = (double) estimatedTotalVectors / maxParts;
        Integer[] biggestFirst = new Integer[parts.length];
        Arrays.setAll(biggestFirst, i -> i);
        Arrays.sort(biggestFirst, Comparator.comparingLong((Integer i) -> estimatedVectorsToScore[i]).reversed());
        int partsLeft = maxParts;
        for (int leaf : biggestFirst) {
            long vectorsToScore = estimatedVectorsToScore[leaf];
            if (vectorsToScore < vectorsPerTask) {
                break;
            }
            if (splittable[leaf]) {
                long scoringPct = 100 * vectorsToScore / estimatedTotalVectors;
                long shareOfTasks = Math.round(vectorsToScore / vectorsPerTask);
                long mostParts = Math.min(scoringPct / MIN_SCORING_PCT_PER_PART, vectorsToScore / MIN_VECTORS_TO_SCORE_PER_PART);
                parts[leaf] = (int) Math.max(1, Math.min(Math.min(shareOfTasks, mostParts), partsLeft));
            }
            partsLeft = Math.max(0, partsLeft - parts[leaf]);
        }
        return parts;
    }

    /** How many of the leaf's vectors the query searches: all of the field's, unless it searches a data slice. */
    private int searchedVectors(LeafReaderContext ctx) throws IOException {
        LeafReader reader = ctx.reader();
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(query.field);
        if (fieldInfo == null) {
            return 0;
        }
        KnnVectorValues values = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE
            ? reader.getByteVectorValues(query.field)
            : reader.getFloatVectorValues(query.field);
        int numVectors = values == null ? 0 : values.size();
        // a leaf too small to split is not worth the lookup of how much of it a data slice query searches
        return numVectors < minVectorsToSplit ? numVectors : query.searchedVectors(ctx, numVectors);
    }

    /** How many vectors a search of that many is expected to score: its visit budget, in posting list entries. */
    private long estimateVectorsToScore(int numVectors) {
        if (numVectors == 0) {
            return 0;
        }
        // a visit ratio of 0 asks for the dynamic one, which the reader works out per segment
        float ratio = visitRatio == 0f ? IVFVectorsReader.segmentVisitRatio(query.numCands, query.k, numVectors) : visitRatio;
        return IVFVectorsReader.visitBudget(ratio, numVectors);
    }

    /** Whether the leaf is searched by scanning DiskBBQ posting lists: a field can hold segments of other formats. */
    private boolean scansPostingLists(LeafReader reader) {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            return false;
        }
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            vectorsReader = perField.getFieldReader(query.field);
        }
        return vectorsReader instanceof IVFVectorsReader<?> ivf && ivf.hasIvfStructure(query.field);
    }

    /** Searches a split leaf as one of its parts: with a collector of its own, through the leaf's scan. */
    private void searchPart(Leaf leaf) throws IOException {
        // no ThreadConfinedArena, unlike the whole-leaf search: other parts' threads read what this part allocates
        LeafInput input = leaf.input;
        AbstractIVFKnnVectorQuery.PartSearch partSearch = leaf.partSearch;
        IVFKnnSearchStrategy strategy = input.collectorManager()
            .newPartSearchStrategy(visitRatio, query.numCands, query.k, partSearch.searchesQueryVector(), leaf.scan);
        AbstractMaxScoreKnnCollector collector = input.collectorManager().newCollector(Integer.MAX_VALUE, strategy, input.ctx());
        if (collector == null) {
            return; // nothing to collect in this leaf, e.g. a nested field's leaf without parent docs
        }
        strategy.setCollector(collector);
        partSearch.search().accept(collector);
    }

    private static final class Leaf {
        private final LeafInput input;
        private final long estimatedVectorsToScore;
        private final int parts;
        private final IVFSegmentScan scan; // shared by the leaf's parts; null unless the leaf is split
        private volatile AbstractIVFKnnVectorQuery.PartSearch partSearch; // how to search a part; set before the parts start
        private volatile TopDocs wholeLeafHits;

        Leaf(LeafInput input, long estimatedVectorsToScore, int parts) {
            this.input = input;
            this.estimatedVectorsToScore = estimatedVectorsToScore;
            this.parts = parts;
            this.scan = parts > 1 ? new IVFSegmentScan(parts) : null;
        }

        /** The leaf's hits, with shard-wide doc ids and each doc once. A split leaf's are those its scan gathered. */
        TopDocs hits() {
            if (scan == null) {
                return wholeLeafHits != null ? wholeLeafHits : AbstractIVFKnnVectorQuery.NO_RESULTS;
            }
            KnnCollector collector = scan.finalCollector();
            if (collector == null) {
                return AbstractIVFKnnVectorQuery.NO_RESULTS;
            }
            TopDocs hits = collector instanceof BulkKnnCollector bulk ? bulk.unsortedTopK() : collector.topDocs();
            if (hits == null || hits.scoreDocs.length == 0) {
                return AbstractIVFKnnVectorQuery.NO_RESULTS; // as a whole-leaf search returns when it finds nothing
            }
            TotalHits visited = new TotalHits(scan.visitedCount(), hits.totalHits.relation());
            return AbstractIVFKnnVectorQuery.keepBestCopies(new TopDocs(visited, hits.scoreDocs), input.ctx().docBase);
        }
    }
}
