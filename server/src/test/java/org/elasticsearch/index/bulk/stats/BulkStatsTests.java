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

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BulkStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        BulkStats stats = new BulkStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        BulkStats read = new BulkStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getTotal(), read.getTotal());
        assertEquals(stats.getTotalTime(), read.getTotalTime());
        assertEquals(stats.getTotalSizeInBytes(), read.getTotalSizeInBytes());
    }

    public void testAddTotals() {
        BulkStats bulkStats1 = new BulkStats(1, 1, 1);
        BulkStats bulkStats2 = new BulkStats(1, 1, 1);

        // adding these two bulk stats and checking stats are correct
        bulkStats1.add(bulkStats2);
        assertStats(bulkStats1, 2);

        // another call, adding again ...
        bulkStats1.add(bulkStats2);
        assertStats(bulkStats1, 3);

    }

    private static void assertStats(BulkStats stats, long equalTo) {
        assertEquals(equalTo, stats.getTotal());
        assertEquals(equalTo, stats.getTotalTimeInMillis());
        assertEquals(equalTo, stats.getTotalSizeInBytes());
    }
}

