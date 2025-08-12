/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class LuceneSliceQueueTests extends ESTestCase {

    public void testBasics() {

        LeafReaderContext leaf1 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf2 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf3 = new MockLeafReader(1000).getContext();
        LeafReaderContext leaf4 = new MockLeafReader(1000).getContext();
        var slice1 = new LuceneSlice(0, null, List.of(new PartialLeafReaderContext(leaf1, 0, 10)), null, null);

        var slice2 = new LuceneSlice(1, null, List.of(new PartialLeafReaderContext(leaf2, 0, 10)), null, null);
        var slice3 = new LuceneSlice(2, null, List.of(new PartialLeafReaderContext(leaf2, 10, 20)), null, null);

        var slice4 = new LuceneSlice(3, null, List.of(new PartialLeafReaderContext(leaf3, 0, 20)), null, null);
        var slice5 = new LuceneSlice(4, null, List.of(new PartialLeafReaderContext(leaf3, 10, 20)), null, null);
        var slice6 = new LuceneSlice(5, null, List.of(new PartialLeafReaderContext(leaf3, 20, 30)), null, null);

        var slice7 = new LuceneSlice(6, null, List.of(new PartialLeafReaderContext(leaf4, 0, 10)), null, null);
        var slice8 = new LuceneSlice(7, null, List.of(new PartialLeafReaderContext(leaf4, 10, 20)), null, null);
        List<LuceneSlice> sliceList = List.of(slice1, slice2, slice3, slice4, slice5, slice6, slice7, slice8);
        // single driver
        {
            LuceneSliceQueue queue = new LuceneSliceQueue(sliceList, Map.of());
            LuceneSlice last = null;
            for (LuceneSlice slice : sliceList) {
                last = queue.nextSlice(last);
                assertEquals(slice, last);
            }
            assertNull(queue.nextSlice(randomBoolean() ? last : null));
        }
        // two drivers
        {
            LuceneSliceQueue queue = new LuceneSliceQueue(sliceList, Map.of());
            LuceneSlice first = null;
            LuceneSlice second = null;
            first = queue.nextSlice(first);
            assertEquals(slice1, first);
            first = queue.nextSlice(first);
            assertEquals(slice2, first);

            second = queue.nextSlice(second);
            assertEquals(slice4, second);
            second = queue.nextSlice(second);
            assertEquals(slice5, second);

            first = queue.nextSlice(first);
            assertEquals(slice3, first);
            second = queue.nextSlice(second);
            assertEquals(slice6, second);
            first = queue.nextSlice(first);
            assertEquals(slice7, first);

            assertEquals(slice8, queue.nextSlice(randomFrom(first, second)));

            assertNull(queue.nextSlice(first));
            assertNull(queue.nextSlice(second));
        }
    }

    public void testRandom() throws Exception {
        List<LuceneSlice> sliceList = new ArrayList<>();
        int numShards = randomIntBetween(1, 10);
        int slicePosition = 0;
        for (int shard = 0; shard < numShards; shard++) {
            int numSegments = randomIntBetween(1, 10);
            for (int segment = 0; segment < numSegments; segment++) {
                int numSlices = randomBoolean() ? 1 : between(2, 5);
                LeafReaderContext leafContext = new MockLeafReader(randomIntBetween(1000, 2000)).getContext();
                for (int i = 0; i < numSlices; i++) {
                    final int minDoc = i * 10;
                    final int maxDoc = minDoc + 10;
                    LuceneSlice slice = new LuceneSlice(
                        slicePosition++,
                        mock(ShardContext.class),
                        List.of(new PartialLeafReaderContext(leafContext, minDoc, maxDoc)),
                        null,
                        null
                    );
                    sliceList.add(slice);
                }
            }
        }
        LuceneSliceQueue queue = new LuceneSliceQueue(sliceList, Map.of());
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
                        if (prevLeaf == currentLeaf) {
                            assertThat(prevLeaf.minDoc(), Matchers.lessThanOrEqualTo(processedSlices.get(i).leaves().getFirst().maxDoc()));
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

    static class MockLeafReader extends LeafReader {
        private final int maxDoc;

        MockLeafReader(int maxDoc) {
            this.maxDoc = maxDoc;
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
            throw new UnsupportedOperationException();
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
        public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
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
}
