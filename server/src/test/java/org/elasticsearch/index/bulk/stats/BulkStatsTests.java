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
import org.elasticsearch.test.ESTestCase;

public class BulkStatsTests extends AbstractWireSerializingTestCase<BulkStats> {

    @Override
    protected Writeable.Reader<BulkStats> instanceReader() {
        return BulkStats::new;
    }

    @Override
    protected BulkStats createTestInstance() {
        return new BulkStats(randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong());
    }

    @Override
    protected BulkStats mutateInstance(BulkStats instance) {
        switch (between(0, 4)) {
            case 0:
                return new BulkStats(randomValueOtherThan(instance.getTotalOperations(), ESTestCase::randomNonNegativeLong),
                    instance.getTotalTimeInMillis(),
                    instance.getTotalSizeInBytes(),
                    instance.getAvgTimeInMillis(),
                    instance.getAvgTimeInMillis());
            case 1:
                return new BulkStats(instance.getTotalOperations(),
                    randomValueOtherThan(instance.getTotalTimeInMillis(), ESTestCase::randomNonNegativeLong),
                    instance.getTotalSizeInBytes(),
                    instance.getAvgTimeInMillis(),
                    instance.getAvgTimeInMillis());
            case 2:
                return new BulkStats(instance.getTotalOperations(),
                    instance.getTotalTimeInMillis(),
                    randomValueOtherThan(instance.getTotalSizeInBytes(), ESTestCase::randomNonNegativeLong),
                    instance.getAvgTimeInMillis(),
                    instance.getAvgTimeInMillis());
            case 3:
                return new BulkStats(instance.getTotalOperations(),
                    instance.getTotalTimeInMillis(),
                    instance.getTotalSizeInBytes(),
                    randomValueOtherThan(instance.getAvgTimeInMillis(), ESTestCase::randomNonNegativeLong),
                    instance.getAvgTimeInMillis());
            case 4:
                return new BulkStats(instance.getTotalOperations(),
                    instance.getTotalTimeInMillis(),
                    instance.getTotalSizeInBytes(),
                    instance.getAvgTimeInMillis(),
                    randomValueOtherThan(instance.getAvgSizeInBytes(), ESTestCase::randomNonNegativeLong));
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    public void testAddTotals() {
        BulkStats bulkStats1 = new BulkStats(1, 1, 1, 2, 2);
        BulkStats bulkStats2 = new BulkStats(1, 1, 1, 2, 2);

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

