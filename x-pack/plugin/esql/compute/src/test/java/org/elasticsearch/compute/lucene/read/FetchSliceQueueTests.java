/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.compute.lucene.read.FetchSliceQueue.SLICE_DOCS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchSliceQueueTests extends ESTestCase {
    /**
     * A long run of a segment goes in slices of {@link FetchSliceQueue#SLICE_DOCS} documents, whether its documents lie next
     * to each other or far apart.
     */
    public void testCutsARunIntoSlicesOfOneSize() {
        int rest = between(1, SLICE_DOCS - 1);
        FetchSliceQueue queue = queue(shard(0, run(0, 0, 2 * SLICE_DOCS + rest, between(1, 100))));
        assertThat(sizes(queue), equalTo(List.of(SLICE_DOCS, SLICE_DOCS, rest)));
    }

    /**
     * A slice holds documents of one segment, so a page loads with one reader.
     */
    public void testASliceStaysInOneSegment() {
        FetchSliceQueue queue = queue(shard(0, run(0, 0, 20, 1), run(1, 0, 20, 1)));
        List<FetchSliceQueue.Slice> slices = inOrder(queue);
        assertThat(slices.stream().map(FetchSliceQueue.Slice::segment).toList(), equalTo(List.of(0, 1)));
        assertThat(sizes(slices), equalTo(List.of(20, 20)));
    }

    /**
     * The slices, one after the other, hold the documents of the request in its order, numbered from 0.
     */
    public void testTheSlicesFollowTheRequestOrder() {
        List<FetchDocsSourceOperator.ShardDocs> shards = randomShards();
        FetchSliceQueue queue = FetchSliceQueue.create(shards);

        List<FetchSliceQueue.Slice> slices = inOrder(queue);
        List<int[]> fromSlices = new ArrayList<>();
        for (int s = 0; s < slices.size(); s++) {
            FetchSliceQueue.Slice slice = slices.get(s);
            assertThat(slice.position(), equalTo(s));
            for (int doc : slice.docs()) {
                fromSlices.add(new int[] { slice.shard(), slice.segment(), doc });
            }
        }
        List<int[]> requested = new ArrayList<>();
        for (FetchDocsSourceOperator.ShardDocs shard : shards) {
            for (int d = 0; d < shard.docCount(); d++) {
                requested.add(new int[] { shard.shard(), shard.segments()[d], shard.docs()[d] });
            }
        }
        assertThat(fromSlices.size(), equalTo(requested.size()));
        for (int r = 0; r < requested.size(); r++) {
            assertArrayEquals(requested.get(r), fromSlices.get(r));
        }
    }

    /**
     * A driver keeps to the slice after the one it loaded, so one driver alone loads the slices in request order.
     */
    public void testADriverKeepsToTheSliceAfterItsLast() {
        FetchSliceQueue queue = FetchSliceQueue.create(randomShards());
        FetchSliceQueue.Slice slice = queue.nextSlice(null);
        for (int position = 0; position < queue.sliceCount(); position++) {
            assertThat(slice.position(), equalTo(position));
            slice = queue.nextSlice(slice);
        }
        assertThat(slice, nullValue());
    }

    /**
     * A driver without a next slice starts on a shard no driver started, then on a segment no driver started, and only
     * then takes any slice that is left.
     */
    public void testADriverStartsAShardThenASegmentThenAnySlice() {
        // shard 0: segment 0 in slices 0 to 2, segment 1 in slice 3. Shard 1: segment 0 in slices 4 and 5.
        FetchSliceQueue queue = queue(shard(0, run(0, 0, 3 * SLICE_DOCS, 1), run(1, 0, 1, 1)), shard(1, run(0, 0, 2 * SLICE_DOCS, 1)));
        List<Integer> taken = new ArrayList<>();
        for (FetchSliceQueue.Slice slice = queue.nextSlice(null); slice != null; slice = queue.nextSlice(null)) {
            taken.add(slice.position());
        }
        assertThat(taken, equalTo(List.of(0, 4, 3, 1, 2, 5)));
    }

    /**
     * When another driver took the slice after its last, a driver moves on to what is left.
     */
    public void testADriverMovesOnWhenAnotherTookItsNextSlice() {
        FetchSliceQueue queue = queue(shard(0, run(0, 0, 3 * SLICE_DOCS, 1)));
        FetchSliceQueue.Slice first = queue.nextSlice(null);
        FetchSliceQueue.Slice stolen = queue.nextSlice(null);
        assertThat(first.position(), equalTo(0));
        assertThat(stolen.position(), equalTo(1));
        assertThat(queue.nextSlice(first).position(), equalTo(2));
        assertThat(queue.nextSlice(stolen), nullValue());
    }

    /**
     * Drivers on several threads take every slice, and each slice once.
     */
    public void testEverySliceGoesToOneDriver() {
        FetchSliceQueue queue = FetchSliceQueue.create(randomShards());
        int drivers = between(2, 8);
        List<List<Integer>> takenByDriver = new ArrayList<>();
        for (int d = 0; d < drivers; d++) {
            takenByDriver.add(new ArrayList<>());
        }
        runInParallel(drivers, d -> {
            for (FetchSliceQueue.Slice slice = queue.nextSlice(null); slice != null; slice = queue.nextSlice(slice)) {
                takenByDriver.get(d).add(slice.position());
            }
        });
        List<Integer> all = new ArrayList<>();
        takenByDriver.forEach(all::addAll);
        Collections.sort(all);
        List<Integer> expected = new ArrayList<>();
        for (int position = 0; position < queue.sliceCount(); position++) {
            expected.add(position);
        }
        assertThat(all, equalTo(expected));
    }

    public void testNoDocumentsMakeNoSlices() {
        FetchSliceQueue queue = queue(new FetchDocsSourceOperator.ShardDocs(0, new int[0], new int[0]));
        assertThat(queue.sliceCount(), equalTo(0));
        assertThat(queue.nextSlice(null), nullValue());
    }

    private static FetchSliceQueue queue(FetchDocsSourceOperator.ShardDocs... shards) {
        return FetchSliceQueue.create(List.of(shards));
    }

    /**
     * {@code count} documents of {@code segment}, from {@code firstDoc}, {@code step} apart.
     */
    private static int[][] run(int segment, int firstDoc, int count, int step) {
        int[][] run = new int[2][count];
        for (int d = 0; d < count; d++) {
            run[0][d] = segment;
            run[1][d] = firstDoc + d * step;
        }
        return run;
    }

    private static FetchDocsSourceOperator.ShardDocs shard(int shard, int[][]... runs) {
        int count = 0;
        for (int[][] run : runs) {
            count += run[0].length;
        }
        int[] segments = new int[count];
        int[] docs = new int[count];
        int at = 0;
        for (int[][] run : runs) {
            System.arraycopy(run[0], 0, segments, at, run[0].length);
            System.arraycopy(run[1], 0, docs, at, run[1].length);
            at += run[0].length;
        }
        return new FetchDocsSourceOperator.ShardDocs(shard, segments, docs);
    }

    /**
     * Shards in random order with a few segments each.
     */
    private static List<FetchDocsSourceOperator.ShardDocs> randomShards() {
        List<Integer> slots = new ArrayList<>();
        for (int s = between(1, 5); s > 0; s--) {
            slots.add(s);
        }
        Collections.shuffle(slots, random());
        List<FetchDocsSourceOperator.ShardDocs> shards = new ArrayList<>();
        for (int slot : slots) {
            List<int[][]> runs = new ArrayList<>();
            int segment = 0;
            for (int r = between(1, 4); r > 0; r--) {
                segment += between(0, 3);
                runs.add(run(segment, between(0, 10), between(1, 3 * SLICE_DOCS), between(1, 50)));
                segment++;
            }
            shards.add(shard(slot, runs.toArray(int[][][]::new)));
        }
        return shards;
    }

    /**
     * Every slice of {@code queue}, sorted by position.
     */
    private static List<FetchSliceQueue.Slice> inOrder(FetchSliceQueue queue) {
        List<FetchSliceQueue.Slice> slices = new ArrayList<>();
        for (FetchSliceQueue.Slice slice = queue.nextSlice(null); slice != null; slice = queue.nextSlice(null)) {
            slices.add(slice);
        }
        slices.sort(Comparator.comparingInt(FetchSliceQueue.Slice::position));
        return slices;
    }

    private static List<Integer> sizes(FetchSliceQueue queue) {
        return sizes(inOrder(queue));
    }

    private static List<Integer> sizes(List<FetchSliceQueue.Slice> slices) {
        return slices.stream().map(slice -> slice.docs().length).toList();
    }
}
