/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.codec.tsdb.PrefixedPartitionsWriter;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests.simpleReader;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

public class LuceneSliceQueueTests extends ESTestCase {

    public void testBasics() {
        var shardContext = new LuceneSourceOperatorTests.MockShardContext(null);
        LeafReaderContext leaf1 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf2 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf3 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf4 = new MockLeafReader(1000).getContext();
        List<Object> query1 = List.of("1");
        List<Object> query2 = List.of("q2");
        List<LuceneSlice> sliceList = List.of(
            // query1: new segment
            new LuceneSlice(
                0,
                true,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf1, 0, 10)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                1,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf2, 0, 10)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                2,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf2, 10, 20)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            // query1: new segment
            new LuceneSlice(
                3,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 0, 20)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                4,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 10, 20)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                5,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 20, 30)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            // query1: new segment
            new LuceneSlice(
                6,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf4, 0, 10)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                7,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf4, 10, 20)),
                null,
                query1,
                LuceneSlice.NEVER_BLOCKED
            ),
            // query2: new segment
            new LuceneSlice(
                8,
                true,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf2, 0, 10)),
                null,
                query2,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                9,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf2, 10, 20)),
                null,
                query2,
                LuceneSlice.NEVER_BLOCKED
            ),
            // query1: new segment
            new LuceneSlice(
                10,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 0, 20)),
                null,
                query2,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                11,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 10, 20)),
                null,
                query2,
                LuceneSlice.NEVER_BLOCKED
            ),
            new LuceneSlice(
                12,
                false,
                shardContext,
                List.of(new PartialLeafReaderContext(leaf3, 20, 30)),
                null,
                query2,
                LuceneSlice.NEVER_BLOCKED
            )
        );
        // single driver
        {
            LuceneSliceQueue queue = new LuceneSliceQueue(index -> shardContext, sliceList, Map.of());
            LuceneSlice last = null;
            for (LuceneSlice slice : sliceList) {
                last = queue.nextSlice(last);
                assertEquals(slice, last);
            }
            assertNull(queue.nextSlice(randomBoolean() ? last : null));
        }
        // three drivers
        {
            LuceneSliceQueue queue = new LuceneSliceQueue(index -> shardContext, sliceList, Map.of());

            LuceneSlice first = null;
            LuceneSlice second = null;
            LuceneSlice third = null;
            first = queue.nextSlice(first);
            assertEquals(sliceList.get(0), first);
            first = queue.nextSlice(first);
            assertEquals(sliceList.get(1), first);

            second = queue.nextSlice(second);
            assertEquals(sliceList.get(8), second);
            second = queue.nextSlice(second);
            assertEquals(sliceList.get(9), second);

            first = queue.nextSlice(first);
            assertEquals(sliceList.get(2), first);
            third = queue.nextSlice(third);
            assertEquals(sliceList.get(3), third);
            first = queue.nextSlice(first);
            assertEquals(sliceList.get(6), first);

            first = queue.nextSlice(first);
            assertEquals(sliceList.get(7), first);
            third = queue.nextSlice(third);
            assertEquals(sliceList.get(4), third);
            first = queue.nextSlice(first);
            assertEquals(sliceList.get(10), first);
            first = queue.nextSlice(first);
            assertEquals(sliceList.get(11), first);
            second = queue.nextSlice(second);
            assertEquals(sliceList.get(5), second);
            second = queue.nextSlice(second);
            assertEquals(sliceList.get(12), second);
            assertNull(null, queue.nextSlice(randomFrom(sliceList)));
        }
    }

    /**
     * Each shard counts its slices until they are read: a slice is read once the next one is asked for after it, or
     * once it is given back unfinished.
     */
    public void testUnfinishedSlicesPerShard() {
        var first = new LuceneSourceOperatorTests.MockShardContext(null);
        var second = new LuceneSourceOperatorTests.MockShardContext(null);
        LeafReaderContext leaf = new MockLeafReader(100).getContext();
        List<LuceneSlice> sliceList = new ArrayList<>();
        for (var shard : List.of(first, first, second)) {
            sliceList.add(
                new LuceneSlice(
                    sliceList.size(),
                    false,
                    shard,
                    List.of(new PartialLeafReaderContext(leaf, 0, 100)),
                    null,
                    null,
                    LuceneSlice.NEVER_BLOCKED
                )
            );
        }
        LuceneSliceQueue queue = new LuceneSliceQueue(i -> first, sliceList, Map.of());
        assertThat(first.unfinishedSlices(), equalTo(2));
        assertThat(second.unfinishedSlices(), equalTo(1));

        LuceneSlice slice = queue.nextSlice(null);
        assertThat(first.unfinishedSlices() + second.unfinishedSlices(), equalTo(3));
        int read = 0;
        LuceneSlice next;
        while ((next = queue.nextSlice(slice)) != null) {
            read++;
            assertThat(first.unfinishedSlices() + second.unfinishedSlices(), equalTo(3 - read));
            slice = next;
        }
        assertThat(first.unfinishedSlices() + second.unfinishedSlices(), equalTo(0));

        LuceneSliceQueue abandoned = new LuceneSliceQueue(i -> first, List.of(sliceList.get(0)), Map.of());
        assertThat(first.unfinishedSlices(), equalTo(1));
        abandoned.finished(abandoned.nextSlice(null));
        assertThat(first.unfinishedSlices(), equalTo(0));
    }

    /**
     * Once the last operator reading a queue stops, for example on reaching a limit, the slices nobody took count as read.
     */
    public void testSlicesLeftWhenTheLastReaderStopsCountAsRead() {
        var shard = new LuceneSourceOperatorTests.MockShardContext(null);
        LeafReaderContext leaf = new MockLeafReader(100).getContext();
        List<LuceneSlice> sliceList = new ArrayList<>();
        int numSlices = between(2, 10);
        for (int i = 0; i < numSlices; i++) {
            sliceList.add(
                new LuceneSlice(i, false, shard, List.of(new PartialLeafReaderContext(leaf, 0, 100)), null, null, LuceneSlice.NEVER_BLOCKED)
            );
        }
        LuceneSliceQueue queue = new LuceneSliceQueue(i -> shard, sliceList, Map.of());
        queue.addReader();
        queue.addReader();
        LuceneSlice read = queue.nextSlice(null);
        queue.finished(read);
        assertThat(shard.unfinishedSlices(), equalTo(numSlices - 1));
        queue.removeReader();
        assertThat(shard.unfinishedSlices(), equalTo(numSlices - 1));
        queue.removeReader();
        assertThat(shard.unfinishedSlices(), equalTo(0));
        assertNull(queue.nextSlice(null));
    }

    public void testRandom() throws Exception {
        List<LuceneSlice> sliceList = new ArrayList<>();
        int numShards = randomIntBetween(1, 10);
        int slicePosition = 0;
        List<ShardContext> shardContexts = new ArrayList<>(numShards);
        for (int shard = 0; shard < numShards; shard++) {
            var shardContext = new LuceneSourceOperatorTests.MockShardContext(null);
            shardContexts.add(shardContext);
            int numSegments = randomIntBetween(1, 10);
            for (int segment = 0; segment < numSegments; segment++) {
                int numSlices = between(10, 50);
                LeafReaderContext leafContext = new MockLeafReader(randomIntBetween(1000, 2000)).getContext();
                for (int i = 0; i < numSlices; i++) {
                    final int minDoc = i * 10;
                    final int maxDoc = minDoc + 10;
                    LuceneSlice slice = new LuceneSlice(
                        slicePosition++,
                        false,
                        shardContext,
                        List.of(new PartialLeafReaderContext(leafContext, minDoc, maxDoc)),
                        null,
                        null,
                        LuceneSlice.NEVER_BLOCKED
                    );
                    sliceList.add(slice);
                }
            }
        }
        LuceneSliceQueue queue = new LuceneSliceQueue(shardContexts::get, sliceList, Map.of());
        Queue<LuceneSlice> allProcessedSlices = ConcurrentCollections.newQueue();
        int numDrivers = randomIntBetween(1, 5);
        CyclicBarrier barrier = new CyclicBarrier(numDrivers + 1);
        List<Thread> drivers = new ArrayList<>();
        for (int d = 0; d < numDrivers; d++) {
            Thread driver = new Thread(() -> {
                try {
                    barrier.await(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                LuceneSlice nextSlice = null;
                List<LuceneSlice> processedSlices = new ArrayList<>();
                while ((nextSlice = queue.nextSlice(nextSlice)) != null) {
                    processedSlices.add(nextSlice);
                }
                allProcessedSlices.addAll(processedSlices);
                // slices from a single driver are forward-only
                for (int i = 1; i < processedSlices.size(); i++) {
                    var currentLeaf = processedSlices.get(i).getLeaf(0);
                    for (int p = 0; p < i; p++) {
                        PartialLeafReaderContext prevLeaf = processedSlices.get(p).getLeaf(0);
                        if (prevLeaf.leafReaderContext() == currentLeaf.leafReaderContext()) {
                            assertThat(prevLeaf.maxDoc(), Matchers.lessThanOrEqualTo(currentLeaf.minDoc()));
                        }
                    }
                }
            });
            drivers.add(driver);
            driver.start();
        }
        barrier.await();
        for (Thread driver : drivers) {
            driver.join();
        }
        assertThat(allProcessedSlices, Matchers.hasSize(sliceList.size()));
        assertThat(Set.copyOf(allProcessedSlices), equalTo(Set.copyOf(sliceList)));
    }

    public void testDocPartitioningBigSegments() {
        LeafReaderContext leaf1 = new MockLeafReader(250).getContext();
        LeafReaderContext leaf2 = new MockLeafReader(400).getContext();
        LeafReaderContext leaf3 = new MockLeafReader(1_400_990).getContext();
        LeafReaderContext leaf4 = new MockLeafReader(2_100_061).getContext();
        LeafReaderContext leaf5 = new MockLeafReader(1_000_607).getContext();
        var adaptivePartitioner = new LuceneSliceQueue.AdaptivePartitioner(250_000, 5);
        List<List<PartialLeafReaderContext>> slices = adaptivePartitioner.partition(List.of(leaf1, leaf2, leaf3, leaf4, leaf5));
        // leaf4: 2_100_061
        int sliceOffset = 0;
        {
            List<Integer> sliceSizes = List.of(262508, 262508, 262508, 262508, 262508, 262507, 262507, 262507);
            for (Integer sliceSize : sliceSizes) {
                List<PartialLeafReaderContext> slice = slices.get(sliceOffset++);
                assertThat(slice, hasSize(1));
                assertThat(slice.getFirst().leafReaderContext(), equalTo(leaf4));
                assertThat(slice.getFirst().maxDoc() - slice.getFirst().minDoc(), equalTo(sliceSize));
            }
        }
        // leaf3: 1_400_990
        {
            List<Integer> sliceSizes = List.of(280198, 280198, 280198, 280198, 280198);
            for (Integer sliceSize : sliceSizes) {
                List<PartialLeafReaderContext> slice = slices.get(sliceOffset++);
                assertThat(slice, hasSize(1));
                assertThat(slice.getFirst().leafReaderContext(), equalTo(leaf3));
                assertThat(slice.getFirst().maxDoc() - slice.getFirst().minDoc(), equalTo(sliceSize));
            }
        }
        // leaf5: 1_000_600
        {
            List<Integer> sliceSizes = List.of(250151, 250151, 250151, 250154);
            for (Integer sliceSize : sliceSizes) {
                List<PartialLeafReaderContext> slice = slices.get(sliceOffset++);
                assertThat(slice, hasSize(1));
                var partialLeaf = slice.getFirst();
                assertThat(partialLeaf.leafReaderContext(), equalTo(leaf5));
                assertThat(partialLeaf.toString(), partialLeaf.maxDoc() - partialLeaf.minDoc(), equalTo(sliceSize));
            }
        }
        // leaf2 and leaf1
        {
            List<PartialLeafReaderContext> slice = slices.get(sliceOffset++);
            assertThat(slice, hasSize(2));
            assertThat(slice.getFirst().leafReaderContext(), equalTo(leaf2));
            assertThat(slice.getFirst().minDoc(), equalTo(0));
            assertThat(slice.getFirst().maxDoc(), equalTo(Integer.MAX_VALUE));
            assertThat(slice.getLast().leafReaderContext(), equalTo(leaf1));
            assertThat(slice.getLast().minDoc(), equalTo(0));
            assertThat(slice.getLast().maxDoc(), equalTo(Integer.MAX_VALUE));
        }
        assertThat(slices, hasSize(sliceOffset));
    }

    public void testBalancedBinPackOneBigManySmall() {
        // 1 big segment + 100 small segments, target = 2 slices.
        // Worst-fit-decreasing should produce two roughly-equal slices: big alone vs all the smalls.
        LeafReaderContext big = new MockLeafReader(10_000_000).getContext();
        List<LeafReaderContext> all = new ArrayList<>();
        all.add(big);
        for (int i = 0; i < 100; i++) {
            all.add(new MockLeafReader(100_000).getContext());
        }
        List<List<PartialLeafReaderContext>> slices = LuceneSliceQueue.PartitioningStrategy.balancedBinPack(all, Set.of(), 2);
        assertThat(slices, hasSize(2));
        // Each slice's leaf-doc-sum should be approximately 10M.
        long sliceADocs = slices.get(0).stream().mapToLong(p -> p.leafReaderContext().reader().maxDoc()).sum();
        long sliceBDocs = slices.get(1).stream().mapToLong(p -> p.leafReaderContext().reader().maxDoc()).sum();
        assertThat(sliceADocs + sliceBDocs, equalTo(20_000_000L));
        assertThat(Math.abs(sliceADocs - sliceBDocs), Matchers.lessThanOrEqualTo(100_000L));
    }

    public void testBalancedBinPackKeepsGuardedLeavesIsolated() {
        LeafReaderContext guarded = new MockLeafReader(500_000).getContext();
        LeafReaderContext a = new MockLeafReader(200_000).getContext();
        LeafReaderContext b = new MockLeafReader(200_000).getContext();
        List<List<PartialLeafReaderContext>> slices = LuceneSliceQueue.PartitioningStrategy.balancedBinPack(
            List.of(guarded, a, b),
            Set.of(guarded),
            2
        );
        // guarded becomes its own slice; the remaining two get the full target parallelism (one bin
        // each), so total slices can exceed the target — the guarded slice doesn't eat into the
        // budget for the leaves that still need iterating.
        assertThat(slices, hasSize(3));
        assertThat(slices.get(0), hasSize(1));
        assertThat(slices.get(0).getFirst().leafReaderContext(), equalTo(guarded));
        assertThat(slices.get(1), hasSize(1));
        assertThat(slices.get(2), hasSize(1));
    }

    /**
     * Regression for the starvation bug: when guarded (shortcut, O(1)) leaves already meet or exceed
     * the target slice count, the unguarded leaves — the ones that actually need iterating — must
     * still get their own full target-sized parallelism, not be crammed onto a single serialized
     * slice. The guarded slices are independent O(1) work and must not subtract from that budget.
     */
    public void testBalancedBinPackDoesNotStarveUnguardedLeaves() {
        List<LeafReaderContext> guarded = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            guarded.add(new MockLeafReader(100_000).getContext());
        }
        LeafReaderContext a = new MockLeafReader(200_000).getContext();
        LeafReaderContext b = new MockLeafReader(200_000).getContext();
        List<LeafReaderContext> all = new ArrayList<>(guarded);
        all.add(a);
        all.add(b);
        int target = 2;
        List<List<PartialLeafReaderContext>> slices = LuceneSliceQueue.PartitioningStrategy.balancedBinPack(
            all,
            new HashSet<>(guarded),
            target
        );
        // 5 guarded single-leaf slices + the 2 unguarded leaves spread across min(target, 2) = 2 bins.
        assertThat(slices, hasSize(7));
        long unguardedSlices = slices.stream()
            .filter(s -> s.stream().anyMatch(p -> p.leafReaderContext() == a || p.leafReaderContext() == b))
            .count();
        assertThat("unguarded leaves must each get their own slice, not be serialized", unguardedSlices, equalTo(2L));
    }

    public void testDocPartitioningKeepWhole() {
        LeafReaderContext small = new MockLeafReader(400).getContext();
        LeafReaderContext mediumGuarded = new MockLeafReader(800_000).getContext();
        LeafReaderContext largeSplit = new MockLeafReader(1_400_990).getContext();
        var adaptivePartitioner = new LuceneSliceQueue.AdaptivePartitioner(250_000, 5);
        List<List<PartialLeafReaderContext>> slices = adaptivePartitioner.partition(
            List.of(small, mediumGuarded, largeSplit),
            Set.of(mediumGuarded)
        );
        // The guarded leaf is emitted as a single full-leaf slice before any large-segment slices.
        assertThat(slices.get(0), hasSize(1));
        assertThat(slices.get(0).getFirst().leafReaderContext(), equalTo(mediumGuarded));
        assertThat(slices.get(0).getFirst().minDoc(), equalTo(0));
        // Full-leaf bound (not Integer.MAX_VALUE) — matches the convention used by SHARD.
        assertThat(slices.get(0).getFirst().maxDoc(), equalTo(mediumGuarded.reader().maxDoc()));
        // Large unguarded leaf gets split into multiple sub-segment slices.
        long largeSplitSliceCount = slices.stream().filter(s -> s.size() == 1 && s.getFirst().leafReaderContext() == largeSplit).count();
        assertThat(largeSplitSliceCount, greaterThan(1L));
        // Small leaf is grouped via IndexSearcher.slices (its own one-slice group).
        long smallSliceCount = slices.stream().filter(s -> s.stream().anyMatch(p -> p.leafReaderContext() == small)).count();
        assertThat(smallSliceCount, equalTo(1L));
    }

    /**
     * The {@code min_docs_per_slice} override (surfaced as the {@code min_docs_per_slice} query pragma) lets a small
     * index actually exercise DOC partitioning: with the default {@link LuceneSliceQueue#MIN_DOCS_PER_SLICE} floor a
     * one-segment index below the floor collapses to a single slice (no parallelism), but lowering the floor splits the
     * segment into multiple slices. This is what makes DOC/SEGMENT parallelism testable without indexing tens of
     * thousands of documents.
     */
    public void testDocPartitioningHonorsMinDocsPerSliceOverride() throws IOException {
        IndexSearcher searcher = new IndexSearcher(new MultiReader(new MockLeafReader(1_000)));
        int taskConcurrency = 4;
        // Default floor (50k) ≫ the 1_000-doc segment: one slice, no parallelism.
        var defaultSlices = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            null,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE
        );
        assertThat(defaultSlices, hasSize(1));
        // Lowered floor: the segment splits so several drivers can share the scan.
        var overriddenSlices = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            taskConcurrency,
            null,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            100
        );
        assertThat(overriddenSlices.size(), greaterThan(1));
    }

    /**
     * Regression for #158605 / #156243: a multi-segment index whose <em>total</em> doc count is below
     * {@link LuceneSliceQueue#MIN_DOCS_PER_SLICE} must still collapse to a single slice. Without capping the
     * bin-pack target by {@code totalDocs / minDocsPerSlice}, {@code balancedBinPack} would open one bin per
     * segment (up to {@code taskConcurrency}) and over-split.
     */
    public void testDocPartitioningDoesNotOverSplitTinyMultiSegmentIndex() throws IOException {
        IndexSearcher searcher = new IndexSearcher(
            new MultiReader(new MockLeafReader(400), new MockLeafReader(300), new MockLeafReader(300))
        );
        var slices = LuceneSliceQueue.PartitioningStrategy.DOC.groups(
            searcher,
            8,
            null,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE
        );
        assertThat(slices, hasSize(1));
        assertThat(slices.getFirst(), hasSize(3));
    }

    public void testCreateSlice() throws IOException {
        try (var directory = newDirectory()) {
            try (var reader = simpleReader(directory, 1, 1)) {
                List<ShardContext> shardContexts = new ArrayList<>();
                List<List<LuceneSliceQueue.QueryAndTags>> shardQueries = new ArrayList<>();
                for (int shard = 0; shard < 10; shard++) {
                    var shardContext = new LuceneSourceOperatorTests.MockShardContext(reader);
                    shardContexts.add(shardContext);
                    int querySize = randomIntBetween(1, 5);
                    List<LuceneSliceQueue.QueryAndTags> queries = new ArrayList<>();
                    for (int i = 0; i < querySize; i++) {
                        queries.add(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of()));
                    }
                    shardQueries.add(queries);

                }
                LuceneSliceQueue.create(
                    new IndexedByShardIdFromList<>(shardContexts),
                    context -> shardQueries.get(context.index()),
                    DataPartitioning.SEGMENT,
                    (ctx, query) -> LuceneSliceQueue.PartitioningStrategy.SEGMENT,
                    LuceneOperator.SMALL_INDEX_BOUNDARY,
                    10,
                    context -> ScoreMode.COMPLETE
                );
                for (int i = 0; i < shardContexts.size(); i++) {
                    var shardContext = shardContexts.get(i);
                    assertThat(shardContext.stats().stats().getTotal().getSearchLoadRate(), greaterThan(0d));
                }
            }
        }
    }

    public void testTimeSeriesPartitionerSingleSegment() throws IOException {
        var prefixes = new PartitionedDocValues.PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        MultiReader reader = new MultiReader(new MockLeafReader(500, prefixes));
        var partitioner = new LuceneSliceQueue.TimeSeriesPartitioner(null);
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 1, 500, 500);
            assertThat(slices, hasSize(1));
            assertThat(slices, contains(List.of(new PartialLeafReaderContext(reader.leaves().get(0), 0, 500))));
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 3, 200, 200);
            assertThat(slices, hasSize(2));
            assertThat(
                slices,
                contains(
                    List.of(new PartialLeafReaderContext(reader.leaves().get(0), 0, 300)),
                    List.of(new PartialLeafReaderContext(reader.leaves().get(0), 300, 500))
                )
            );
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 5, 100, 100);
            assertThat(slices, hasSize(3));
            assertThat(
                slices,
                contains(
                    List.of(new PartialLeafReaderContext(reader.leaves().get(0), 0, 100)),
                    List.of(new PartialLeafReaderContext(reader.leaves().get(0), 100, 300)),
                    List.of(new PartialLeafReaderContext(reader.leaves().get(0), 300, 500))
                )
            );
        }
    }

    public void testTimeSeriesPartitionerMultipleSegments() throws IOException {
        var prefixes0 = new PartitionedDocValues.PrefixPartitions(2, new int[] { 0, 1 }, new int[] { 0, 100 });
        var prefixes1 = new PartitionedDocValues.PrefixPartitions(2, new int[] { 1, 6 }, new int[] { 0, 160 });
        var prefixes2 = new PartitionedDocValues.PrefixPartitions(5, new int[] { 0, 1, 2, 3, 6 }, new int[] { 0, 50, 120, 250, 470 });
        MultiReader reader = new MultiReader(
            new MockLeafReader(200, prefixes0),
            new MockLeafReader(370, prefixes1),
            new MockLeafReader(801, prefixes2)
        );
        var partitioner = new LuceneSliceQueue.TimeSeriesPartitioner(null);
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 1, 1371, 1371);
            assertThat(slices, hasSize(1));
            assertThat(
                slices.get(0),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(0), 0, 200),
                    new PartialLeafReaderContext(reader.leaves().get(1), 0, 370),
                    new PartialLeafReaderContext(reader.leaves().get(2), 0, 801)
                )
            );
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 2, 750, 750);
            assertThat(slices, hasSize(2));
            assertThat(
                slices.get(0),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(0), 0, 200),
                    new PartialLeafReaderContext(reader.leaves().get(1), 0, 160),
                    new PartialLeafReaderContext(reader.leaves().get(2), 0, 470)
                )
            );
            assertThat(
                slices.get(1),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(1), 160, 370),
                    new PartialLeafReaderContext(reader.leaves().get(2), 470, 801)
                )
            );
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 3, 500, 500);
            assertThat(slices, hasSize(2));
            assertThat(
                slices.get(0),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(0), 0, 200),
                    new PartialLeafReaderContext(reader.leaves().get(1), 0, 160),
                    new PartialLeafReaderContext(reader.leaves().get(2), 0, 250)
                )
            );
            assertThat(
                slices.get(1),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(1), 160, 370),
                    new PartialLeafReaderContext(reader.leaves().get(2), 250, 801)
                )
            );
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 5, 200, 200);
            assertThat(slices, hasSize(3));
            assertThat(
                slices.get(0),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(0), 0, 200),
                    new PartialLeafReaderContext(reader.leaves().get(1), 0, 160),
                    new PartialLeafReaderContext(reader.leaves().get(2), 0, 120)
                )
            );
            assertThat(slices.get(1), containsInAnyOrder(new PartialLeafReaderContext(reader.leaves().get(2), 120, 470)));
            assertThat(
                slices.get(2),
                containsInAnyOrder(
                    new PartialLeafReaderContext(reader.leaves().get(1), 160, 370),
                    new PartialLeafReaderContext(reader.leaves().get(2), 470, 801)
                )
            );
        }
    }

    /**
     * A lower floor gives a small metric more slices, up to one for each task. The cap of docs that the largest
     * leaf gives to a slice wins over a floor above it.
     */
    public void testTimeSeriesPartitionerMinDocsPerSlice() throws IOException {
        MultiReader reader = new MultiReader(new MockLeafReader(1000, evenCells(10, 100)));
        var partitioner = new LuceneSliceQueue.TimeSeriesPartitioner(null);
        assertThat(partitioner.partition(reader.leaves(), 5, 1000, 1000), hasSize(1));
        assertThat(partitioner.partition(reader.leaves(), 5, 100, 1000), hasSize(5));
        assertThat(partitioner.partition(reader.leaves(), 10, 100, 1000), hasSize(10));
        assertThat(partitioner.partition(reader.leaves(), 10, 1, 1000), hasSize(10));
        assertThat(partitioner.partition(reader.leaves(), 1, 5000, 300), hasSize(4));
    }

    /**
     * Only a query that requires a field gets the smaller slices: without one every metric is sliced.
     */
    public void testTimeSeriesSmallerSlicesForRequiredField() throws IOException {
        IndexSearcher searcher = new IndexSearcher(
            new MultiReader(new MockLeafReader(1_000_000, evenCells(10, 100_000), "metric", 0, 1_000_000))
        );
        int taskConcurrency = 10;
        var withoutRequiredField = LuceneSliceQueue.PartitioningStrategy.TIME_SERIES.groups(
            searcher,
            taskConcurrency,
            null,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE
        );
        assertThat(withoutRequiredField, hasSize(4));
        Weight requiresMetric = searcher.createWeight(new FieldExistsQuery("metric"), ScoreMode.COMPLETE_NO_SCORES, 1f);
        var withRequiredField = LuceneSliceQueue.PartitioningStrategy.TIME_SERIES.groups(
            searcher,
            taskConcurrency,
            requiresMetric,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE
        );
        assertThat(withRequiredField, hasSize(10));
        var withLargeFloor = LuceneSliceQueue.PartitioningStrategy.TIME_SERIES.groups(
            searcher,
            taskConcurrency,
            requiresMetric,
            LuceneSliceQueue.LeafSplitGuard.NEVER,
            LuceneSliceQueue.MAX_DOCS_PER_SLICE
        );
        assertThat(withLargeFloor, hasSize(4));
    }

    public void testTimeSeriesPartitionerSkipsMetricsWithoutRequiredField() throws IOException {
        int secondMetric = 1 << (PrefixedPartitionsWriter.PARTITION_PREFIX_BITS - Byte.SIZE);
        int thirdMetric = 2 * secondMetric;
        var cells0 = new PartitionedDocValues.PrefixPartitions(3, new int[] { 0, secondMetric, thirdMetric }, new int[] { 0, 100, 300 });
        var cells1 = new PartitionedDocValues.PrefixPartitions(2, new int[] { secondMetric, thirdMetric }, new int[] { 0, 200 });
        // only docs of the second metric have the field, and only in the first leaf
        MultiReader reader = new MultiReader(new MockLeafReader(400, cells0, "metric", 150, 160), new MockLeafReader(500, cells1));
        assertThat(
            new LuceneSliceQueue.TimeSeriesPartitioner("metric").partition(reader.leaves(), 1, 1, 1000),
            contains(
                List.of(
                    new PartialLeafReaderContext(reader.leaves().get(0), 100, 300),
                    new PartialLeafReaderContext(reader.leaves().get(1), 0, 200)
                )
            )
        );
        assertThat(new LuceneSliceQueue.TimeSeriesPartitioner("other").partition(reader.leaves(), 1, 1, 1000), empty());
        assertThat(new LuceneSliceQueue.TimeSeriesPartitioner(null).partition(reader.leaves(), 1, 1, 1000), hasSize(3));
    }

    /**
     * Every doc lands in exactly one slice, a slice stays within one metric, and a prefix is never split between slices.
     */
    public void testTimeSeriesPartitionerKeepsPrefixesWhole() throws IOException {
        final int shift = PrefixedPartitionsWriter.PARTITION_PREFIX_BITS - Byte.SIZE;
        final int numMetrics = between(1, 5);
        List<MockLeafReader> readers = new ArrayList<>();
        List<PartitionedDocValues.PrefixPartitions> cellsOfLeaves = new ArrayList<>();
        for (int leaf = between(1, 6); leaf > 0; leaf--) {
            List<Integer> prefixes = new ArrayList<>();
            List<Integer> startDocs = new ArrayList<>();
            int maxDoc = 0;
            for (int metric = 0; metric < numMetrics; metric++) {
                if (prefixes.isEmpty() == false && randomBoolean()) {
                    continue;
                }
                for (int low = between(0, 200); low < 1 << shift; low += between(1, 300)) {
                    prefixes.add((metric << shift) + low);
                    startDocs.add(maxDoc);
                    maxDoc += between(1, 50);
                }
            }
            var cells = new PartitionedDocValues.PrefixPartitions(
                prefixes.size(),
                prefixes.stream().mapToInt(Integer::intValue).toArray(),
                startDocs.stream().mapToInt(Integer::intValue).toArray()
            );
            cellsOfLeaves.add(cells);
            readers.add(new MockLeafReader(maxDoc, cells));
        }
        MultiReader reader = new MultiReader(readers.toArray(MockLeafReader[]::new));
        var slices = new LuceneSliceQueue.TimeSeriesPartitioner(null).partition(
            reader.leaves(),
            between(1, 8),
            between(1, 500),
            between(1, 2000)
        );
        int[] nextDoc = new int[readers.size()];
        int lastPrefixOfPreviousSlice = -1;
        for (List<PartialLeafReaderContext> slice : slices) {
            int firstPrefix = Integer.MAX_VALUE;
            int lastPrefix = -1;
            for (PartialLeafReaderContext range : slice) {
                int leaf = range.leafReaderContext().ord;
                var cells = cellsOfLeaves.get(leaf);
                assertThat("docs of a leaf are handed out in order, once", range.minDoc(), equalTo(nextDoc[leaf]));
                assertThat(range.maxDoc(), greaterThan(range.minDoc()));
                nextDoc[leaf] = range.maxDoc();
                int firstCell = Arrays.binarySearch(cells.startDocs(), 0, cells.numPartitions(), range.minDoc());
                assertThat("a range starts at a cell", firstCell, greaterThanOrEqualTo(0));
                int endCell = range.maxDoc() == readers.get(leaf).maxDoc()
                    ? cells.numPartitions()
                    : Arrays.binarySearch(cells.startDocs(), 0, cells.numPartitions(), range.maxDoc());
                assertThat("a range ends at a cell", endCell, greaterThan(firstCell));
                firstPrefix = Math.min(firstPrefix, cells.prefixes()[firstCell]);
                lastPrefix = Math.max(lastPrefix, cells.prefixes()[endCell - 1]);
            }
            assertThat("a slice stays within one metric", firstPrefix >>> shift, equalTo(lastPrefix >>> shift));
            assertThat("a prefix is never split between slices", lastPrefixOfPreviousSlice, lessThan(firstPrefix));
            lastPrefixOfPreviousSlice = lastPrefix;
        }
        for (int leaf = 0; leaf < readers.size(); leaf++) {
            assertThat("every doc is in a slice", nextDoc[leaf], equalTo(readers.get(leaf).maxDoc()));
        }
    }

    public void testTimeSeriesRequiredField() {
        Query exists = new FieldExistsQuery("metric");
        assertThat(LuceneSliceQueue.TimeSeriesPartitioner.requiredField(exists), equalTo("metric"));
        assertThat(LuceneSliceQueue.TimeSeriesPartitioner.requiredField(new ConstantScoreQuery(exists)), equalTo("metric"));
        Query nested = new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.FILTER)
            .add(new ConstantScoreQuery(exists), BooleanClause.Occur.FILTER)
            .build();
        assertThat(
            LuceneSliceQueue.TimeSeriesPartitioner.requiredField(new BooleanQuery.Builder().add(nested, BooleanClause.Occur.MUST).build()),
            equalTo("metric")
        );
        Query either = new BooleanQuery.Builder().add(exists, BooleanClause.Occur.SHOULD)
            .add(new FieldExistsQuery("other"), BooleanClause.Occur.SHOULD)
            .build();
        assertNull(LuceneSliceQueue.TimeSeriesPartitioner.requiredField(either));
        Query excluded = new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.FILTER)
            .add(exists, BooleanClause.Occur.MUST_NOT)
            .build();
        assertNull(LuceneSliceQueue.TimeSeriesPartitioner.requiredField(excluded));
        assertNull(LuceneSliceQueue.TimeSeriesPartitioner.requiredField(Queries.ALL_DOCS_INSTANCE));
    }

    /**
     * The cells of a leaf that holds one metric with {@code numPrefixes} prefixes of {@code docsPerPrefix} docs each.
     */
    private static PartitionedDocValues.PrefixPartitions evenCells(int numPrefixes, int docsPerPrefix) {
        int[] prefixes = new int[numPrefixes];
        int[] startDocs = new int[numPrefixes];
        for (int i = 0; i < numPrefixes; i++) {
            prefixes[i] = i;
            startDocs[i] = i * docsPerPrefix;
        }
        return new PartitionedDocValues.PrefixPartitions(numPrefixes, prefixes, startDocs);
    }

    static class MockLeafReader extends LeafReader {
        private final int maxDoc;
        private final PartitionedDocValues.PrefixPartitions prefixPartitions;
        private final String metricField;
        private final int minDocWithMetric;
        private final int maxDocWithMetric;

        MockLeafReader(int maxDoc) {
            this(maxDoc, null);
        }

        MockLeafReader(int maxDoc, PartitionedDocValues.PrefixPartitions prefixPartitions) {
            this(maxDoc, prefixPartitions, null, 0, 0);
        }

        /**
         * A leaf whose docs in {@code [minDocWithMetric, maxDocWithMetric)} have a value for {@code metricField}.
         */
        MockLeafReader(
            int maxDoc,
            PartitionedDocValues.PrefixPartitions prefixPartitions,
            String metricField,
            int minDocWithMetric,
            int maxDocWithMetric
        ) {
            this.maxDoc = maxDoc;
            this.prefixPartitions = prefixPartitions;
            this.metricField = metricField;
            this.minDocWithMetric = minDocWithMetric;
            this.maxDocWithMetric = maxDocWithMetric;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Terms terms(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            if (field.equals("_tsid") == false || prefixPartitions == null) {
                return null;
            }
            return new PartitionedSortedDocValues(prefixPartitions);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            if (field.equals(metricField) == false) {
                throw new UnsupportedOperationException();
            }
            final DocIdSetIterator docs = DocIdSetIterator.range(minDocWithMetric, maxDocWithMetric);
            return new SortedNumericDocValues() {
                @Override
                public long nextValue() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public boolean advanceExact(int target) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int docID() {
                    return docs.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    return docs.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    return docs.advance(target);
                }

                @Override
                public long cost() {
                    return docs.cost();
                }
            };
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldInfos getFieldInfos() {
            if (metricField == null) {
                return FieldInfos.EMPTY;
            }
            return new FieldInfos(
                new FieldInfo[] {
                    new FieldInfo(
                        metricField,
                        0,
                        false,
                        false,
                        false,
                        IndexOptions.NONE,
                        DocValuesType.SORTED_NUMERIC,
                        DocValuesSkipIndexType.NONE,
                        -1,
                        Map.of(),
                        0,
                        0,
                        0,
                        0,
                        VectorEncoding.FLOAT32,
                        VectorSimilarityFunction.EUCLIDEAN,
                        false,
                        false
                    ) }
            );
        }

        @Override
        public Bits getLiveDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeafMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermVectors termVectors() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            return maxDoc;
        }

        @Override
        public int maxDoc() {
            return maxDoc;
        }

        @Override
        public StoredFields storedFields() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }
    }

    static class PartitionedSortedDocValues extends AbstractSortedDocValues implements PartitionedDocValues {
        final PartitionedDocValues.PrefixPartitions prefixPartitions;

        PartitionedSortedDocValues(PrefixPartitions prefixPartitions) {
            this.prefixPartitions = prefixPartitions;
        }

        @Override
        public int ordValue() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getValueCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrefixPartitions prefixPartitions(PrefixPartitions reuse) throws IOException {
            return prefixPartitions;
        }

        @Override
        public int prefixPartitionBits() {
            return PrefixedPartitionsWriter.PARTITION_PREFIX_BITS;
        }
    }
}
