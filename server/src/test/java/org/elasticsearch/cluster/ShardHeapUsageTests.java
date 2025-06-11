/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardHeapUsageTests extends ESTestCase {

    public void testEstimatedUsageAsPercentage() {
        final long totalBytes = randomNonNegativeLong();
        final long estimatedUsageBytes = randomLongBetween(0, totalBytes);
        final ShardHeapUsage shardHeapUsage = new ShardHeapUsage(randomUUID(), totalBytes, estimatedUsageBytes);
        assertThat(shardHeapUsage.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(shardHeapUsage.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(shardHeapUsage.estimatedUsageAsPercentage(), 100.0 * estimatedUsageBytes / totalBytes, 0.0001);
    }

    public void testEstimatedFreeBytesAsPercentage() {
        final long totalBytes = randomNonNegativeLong();
        final long estimatedUsageBytes = randomLongBetween(0, totalBytes);
        final long estimatedFreeBytes = totalBytes - estimatedUsageBytes;
        final ShardHeapUsage shardHeapUsage = new ShardHeapUsage(randomUUID(), totalBytes, estimatedUsageBytes);
        assertThat(shardHeapUsage.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(shardHeapUsage.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(shardHeapUsage.estimatedFreeBytesAsPercentage(), 100.0 * estimatedFreeBytes / totalBytes, 0.0001);
    }
}
