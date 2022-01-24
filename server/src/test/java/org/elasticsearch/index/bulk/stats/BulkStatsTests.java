/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        return new BulkStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected BulkStats mutateInstance(BulkStats instance) {
        return switch (between(0, 4)) {
            case 0 -> new BulkStats(
                randomValueOtherThan(instance.getTotalOperations(), ESTestCase::randomNonNegativeLong),
                instance.getTotalTimeInMillis(),
                instance.getTotalSizeInBytes(),
                instance.getAvgTimeInMillis(),
                instance.getAvgTimeInMillis()
            );
            case 1 -> new BulkStats(
                instance.getTotalOperations(),
                randomValueOtherThan(instance.getTotalTimeInMillis(), ESTestCase::randomNonNegativeLong),
                instance.getTotalSizeInBytes(),
                instance.getAvgTimeInMillis(),
                instance.getAvgTimeInMillis()
            );
            case 2 -> new BulkStats(
                instance.getTotalOperations(),
                instance.getTotalTimeInMillis(),
                randomValueOtherThan(instance.getTotalSizeInBytes(), ESTestCase::randomNonNegativeLong),
                instance.getAvgTimeInMillis(),
                instance.getAvgTimeInMillis()
            );
            case 3 -> new BulkStats(
                instance.getTotalOperations(),
                instance.getTotalTimeInMillis(),
                instance.getTotalSizeInBytes(),
                randomValueOtherThan(instance.getAvgTimeInMillis(), ESTestCase::randomNonNegativeLong),
                instance.getAvgTimeInMillis()
            );
            case 4 -> new BulkStats(
                instance.getTotalOperations(),
                instance.getTotalTimeInMillis(),
                instance.getTotalSizeInBytes(),
                instance.getAvgTimeInMillis(),
                randomValueOtherThan(instance.getAvgSizeInBytes(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("failure, got illegal switch case");
        };
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
