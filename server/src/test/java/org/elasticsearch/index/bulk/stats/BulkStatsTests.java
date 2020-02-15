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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class BulkStatsTests extends AbstractWireSerializingTestCase<BulkStats> {

    @Override
    protected Writeable.Reader<BulkStats> instanceReader() {
        return BulkStats::new;
    }

    @Override
    protected BulkStats createTestInstance() {
        return new BulkStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected BulkStats mutateInstance(BulkStats instance) {
        BulkStats mutateBulkStats = new BulkStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        switch (between(0, 1)) {
            case 0:
                break;
            case 1:
                mutateBulkStats.add(instance);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return mutateBulkStats;
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
        assertEquals(equalTo, stats.getTotalOperations());
        assertEquals(equalTo, stats.getTotalTimeInMillis());
        assertEquals(equalTo, stats.getTotalSizeInBytes());
    }

}

