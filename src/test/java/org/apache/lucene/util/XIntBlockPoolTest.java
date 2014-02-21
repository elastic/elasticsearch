/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.elasticsearch.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Copy of TestIntBlockPool from Lucene to make sure we didn't kill
 * XIntBlockPool when we got it working with our PageCacheRecycler.
 */
public class XIntBlockPoolTest extends ElasticsearchTestCase {

    public void testSingleWriterReader() {
        Counter bytesUsed = Counter.newCounter();
        XIntBlockPool pool = new XIntBlockPool(new ByteTrackingAllocator(bytesUsed));

        for (int j = 0; j < 2; j++) {
            XIntBlockPool.SliceWriter writer = new XIntBlockPool.SliceWriter(pool);
            int start = writer.startNewSlice();
            int num = atLeast(100);
            for (int i = 0; i < num; i++) {
                writer.writeInt(i);
            }

            int upto = writer.getCurrentOffset();
            XIntBlockPool.SliceReader reader = new XIntBlockPool.SliceReader(pool);
            reader.reset(start, upto);
            for (int i = 0; i < num; i++) {
                assertEquals(i, reader.readInt());
            }
            assertTrue(reader.endOfSlice());
            if (getRandom().nextBoolean()) {
                pool.reset(true, false);
                assertEquals(0, bytesUsed.get());
            } else {
                pool.reset(true, true);
                assertEquals(XIntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT, bytesUsed.get());
            }
        }
        pool.release();
    }

    public void testMultipleWriterReader() {
        Counter bytesUsed = Counter.newCounter();
        XIntBlockPool pool = new XIntBlockPool(new ByteTrackingAllocator(bytesUsed));
        for (int j = 0; j < 2; j++) {
            List<StartEndAndValues> holders = new ArrayList<StartEndAndValues>();
            int num = atLeast(4);
            for (int i = 0; i < num; i++) {
                holders.add(new StartEndAndValues(getRandom().nextInt(1000)));
            }
            XIntBlockPool.SliceWriter writer = new XIntBlockPool.SliceWriter(pool);
            XIntBlockPool.SliceReader reader = new XIntBlockPool.SliceReader(pool);

            int numValuesToWrite = atLeast(10000);
            for (int i = 0; i < numValuesToWrite; i++) {
                StartEndAndValues values = holders.get(getRandom().nextInt(holders.size()));
                if (values.valueCount == 0) {
                    values.start = writer.startNewSlice();
                } else {
                    writer.reset(values.end);
                }
                writer.writeInt(values.nextValue());
                values.end = writer.getCurrentOffset();
                if (getRandom().nextInt(5) == 0) {
                    // pick one and reader the ints
                    assertReader(reader, holders.get(getRandom().nextInt(holders.size())));
                }
            }

            while (!holders.isEmpty()) {
                StartEndAndValues values = holders.remove(getRandom().nextInt(holders.size()));
                assertReader(reader, values);
            }
            if (getRandom().nextBoolean()) {
                pool.reset(true, false);
                assertEquals(0, bytesUsed.get());
            } else {
                pool.reset(true, true);
                assertEquals(XIntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT, bytesUsed.get());
            }
        }
        pool.release();
    }

    private static class ByteTrackingAllocator extends XIntBlockPool.Allocator {
        private final Counter bytesUsed;

        public ByteTrackingAllocator(Counter bytesUsed) {
            super(cacheRecycler());
            this.bytesUsed = bytesUsed;
        }

        @Override
        public Recycler.V<int[]> getIntBlock() {
            bytesUsed.addAndGet(XIntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT);
            return super.getIntBlock();
        }

        @Override
        public void recycleIntBlocks(Recycler.V<int[]>[] blocks, int start, int end) {
            bytesUsed.addAndGet(-((end - start) * XIntBlockPool.INT_BLOCK_SIZE * RamUsageEstimator.NUM_BYTES_INT));
            super.recycleIntBlocks(blocks, start, end);
        }
    }

    private void assertReader(XIntBlockPool.SliceReader reader, StartEndAndValues values) {
        reader.reset(values.start, values.end);
        for (int i = 0; i < values.valueCount; i++) {
            assertEquals(values.valueOffset + i, reader.readInt());
        }
        assertTrue(reader.endOfSlice());
    }

    private static class StartEndAndValues {
        int valueOffset;
        int valueCount;
        int start;
        int end;

        public StartEndAndValues(int valueOffset) {
            this.valueOffset = valueOffset;
        }

        public int nextValue() {
            return valueOffset + valueCount++;
        }
    }

    public static PageCacheRecycler cacheRecycler() {
        return new MockPageCacheRecycler(ImmutableSettings.EMPTY, new ThreadPool());
    }
}
