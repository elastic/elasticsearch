/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

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

    /**
     * A lazy queue builds a shard's slices the first time a driver asks and nothing is published yet, keeps that shard's
     * first slice for the asking driver and publishes the rest; concurrent drivers hand every slice out exactly once.
     */
    public void testLazyQueue() throws IOException {
        try (var directory = newDirectory()) {
            try (var reader = simpleReader(directory, 20, 5)) {
                int numShards = between(1, 8);
                int queriesPerShard = between(1, 3);
                List<ShardContext> shardContexts = new ArrayList<>();
                for (int shard = 0; shard < numShards; shard++) {
                    shardContexts.add(new LuceneSourceOperatorTests.MockShardContext(reader, shard));
                }
                List<LuceneSliceQueue.QueryAndTags> queries = new ArrayList<>();
                for (int i = 0; i < queriesPerShard; i++) {
                    queries.add(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of(i)));
                }
                LuceneSliceQueue queue = LuceneSliceQueue.createLazy(
                    new IndexedByShardIdFromList<>(shardContexts),
                    context -> queries,
                    DataPartitioning.SHARD,
                    (ctx, query) -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                    LuceneOperator.SMALL_INDEX_BOUNDARY,
                    between(1, 4),
                    context -> ScoreMode.COMPLETE_NO_SCORES,
                    LuceneSliceQueue.LeafSplitGuard.NEVER,
                    LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                    QueryWarnings.NOOP
                );
                // Nothing is prepared before a driver asks, but the parallelism estimate is exact for SHARD partitioning.
                assertThat(queue.totalSlices(), equalTo(0));
                assertThat(queue.estimatedSlices(), equalTo(numShards * queriesPerShard));
                assertThat(queue.partitioningStrategies().keySet(), empty());
                // Mock shards share one identifier, so compare against the identifiers rather than the shard count.
                Set<String> identifiers = new HashSet<>();
                for (ShardContext shardContext : shardContexts) {
                    identifiers.add(shardContext.shardIdentifier());
                }
                assertThat(queue.shardIdentifiers(), equalTo(identifiers));

                // The first slice handed out is the first shard's query head; the rest of that shard is published.
                LuceneSlice first = queue.nextSlice(null);
                assertNotNull(first);
                assertThat(first.shardContext(), sameInstance(shardContexts.get(0)));
                assertThat(first.queryHead(), equalTo(true));
                assertThat(queue.totalSlices(), equalTo(queriesPerShard));

                // Concurrent drivers drain the rest, each slice exactly once, until every shard is prepared.
                int drivers = between(1, 4);
                Queue<LuceneSlice> taken = ConcurrentCollections.newQueue();
                taken.add(first);
                CyclicBarrier barrier = new CyclicBarrier(drivers);
                Thread[] threads = new Thread[drivers];
                for (int t = 0; t < drivers; t++) {
                    threads[t] = new Thread(() -> {
                        try {
                            barrier.await(30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        LuceneSlice slice = null;
                        while ((slice = queue.nextSlice(slice)) != null) {
                            taken.add(slice);
                        }
                    });
                    threads[t].start();
                }
                for (Thread thread : threads) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
                assertThat(queue.totalSlices(), equalTo(numShards * queriesPerShard));
                assertThat(taken, hasSize(numShards * queriesPerShard));
                Set<Integer> positions = new HashSet<>();
                for (LuceneSlice slice : taken) {
                    assertTrue("slice handed out twice: " + slice.slicePosition(), positions.add(slice.slicePosition()));
                }
                assertThat(queue.partitioningStrategies().keySet(), equalTo(identifiers));
                assertNull(queue.nextSlice(null));
            }
        }
    }

    /**
     * A driver that finds every shard claimed while another driver is still preparing one must wait for that preparation
     * ({@link LuceneSliceQueue#awaitPreparation}) rather than finish: the slices it publishes are for any driver to take.
     */
    public void testLazyQueueWaitsForInFlightPreparation() throws Exception {
        try (var directory = newDirectory()) {
            try (var reader = simpleReader(directory, 20, 5)) {
                List<ShardContext> shardContexts = List.of(
                    new LuceneSourceOperatorTests.MockShardContext(reader, 0),
                    new LuceneSourceOperatorTests.MockShardContext(reader, 1)
                );
                java.util.concurrent.CountDownLatch preparingShard1 = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.CountDownLatch releaseShard1 = new java.util.concurrent.CountDownLatch(1);
                List<LuceneSliceQueue.QueryAndTags> queries = List.of(
                    new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of()),
                    new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
                );
                LuceneSliceQueue queue = LuceneSliceQueue.createLazy(new IndexedByShardIdFromList<>(shardContexts), context -> {
                    if (context.index() == 1) {
                        preparingShard1.countDown();
                        try {
                            assertTrue(releaseShard1.await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                    return queries;
                },
                    DataPartitioning.SHARD,
                    (ctx, query) -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                    LuceneOperator.SMALL_INDEX_BOUNDARY,
                    2,
                    context -> ScoreMode.COMPLETE_NO_SCORES,
                    LuceneSliceQueue.LeafSplitGuard.NEVER,
                    LuceneSliceQueue.MIN_DOCS_PER_SLICE,
                    QueryWarnings.NOOP
                );
                assertNull(queue.awaitPreparation());
                // Driver A takes shard 0 and the first of its two slices; the second is published.
                LuceneSlice a = queue.nextSlice(null);
                assertNotNull(a);
                assertThat(a.shardContext(), sameInstance(shardContexts.get(0)));
                // Driver B takes the published slice, then claims shard 1 and stalls inside its preparation.
                LuceneSlice b = queue.nextSlice(null);
                assertNotNull(b);
                assertThat(b.shardContext(), sameInstance(shardContexts.get(0)));
                Thread driverB = new Thread(() -> {
                    // B keeps shard 1's first slice; the second is published for whoever asks next.
                    LuceneSlice slice = queue.nextSlice(b);
                    assertNotNull(slice);
                    assertThat(slice.shardContext(), sameInstance(shardContexts.get(1)));
                });
                driverB.start();
                assertTrue(preparingShard1.await(30, TimeUnit.SECONDS));
                // Driver A finds nothing published and every shard claimed: it must be told to wait, not to finish.
                assertNull(queue.nextSlice(a));
                var preparing = queue.awaitPreparation();
                assertNotNull(preparing);
                assertFalse(preparing.isDone());
                releaseShard1.countDown();
                safeAwait(preparing);
                driverB.join();
                // Once published, shard 1's second slice is there for driver A, and then the queue is final.
                LuceneSlice fromShard1 = queue.nextSlice(a);
                assertNotNull(fromShard1);
                assertThat(fromShard1.shardContext(), sameInstance(shardContexts.get(1)));
                assertNull(queue.nextSlice(fromShard1));
                assertNull(queue.awaitPreparation());
            }
        }
    }

    public void testTimeSeriesPartitionerSingleSegment() throws IOException {
        var prefixes = new PartitionedDocValues.PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        MultiReader reader = new MultiReader(new MockLeafReader(500, prefixes));
        var partitioner = new LuceneSliceQueue.TimeSeriesPartitioner();
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 1, 500);
            assertThat(slices, hasSize(1));
            assertThat(slices, contains(List.of(new PartialLeafReaderContext(reader.leaves().get(0), 0, 500))));
        }
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 3, 200);
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
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 5, 100);
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
        var partitioner = new LuceneSliceQueue.TimeSeriesPartitioner();
        {
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 1, 1371);
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
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 2, 750);
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
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 3, 500);
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
            List<List<PartialLeafReaderContext>> slices = partitioner.partition(reader.leaves(), 5, 200);
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

    static class MockLeafReader extends LeafReader {
        private final int maxDoc;
        private final PartitionedDocValues.PrefixPartitions prefixPartitions;

        MockLeafReader(int maxDoc) {
            this.maxDoc = maxDoc;
            this.prefixPartitions = null;
        }

        MockLeafReader(int maxDoc, PartitionedDocValues.PrefixPartitions prefixPartitions) {
            this.maxDoc = maxDoc;
            this.prefixPartitions = prefixPartitions;
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            return 1;
        }
    }
}
