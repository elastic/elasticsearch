/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NodeHeapEstimatesTests extends ESTestCase {

    public void testConstructorRejectsNegativeValues() {
        final long totalHeapUsage = randomNonNegativeLong();
        final long hostedShardsHeapUsage = randomNonNegativeLong();
        final long nonShardHeapUsage = randomNonNegativeLong();
        final long negative = randomLongBetween(Long.MIN_VALUE, -1);

        expectThrows(AssertionError.class, () -> new NodeHeapEstimates(negative, hostedShardsHeapUsage, nonShardHeapUsage));
        expectThrows(AssertionError.class, () -> new NodeHeapEstimates(totalHeapUsage, negative, nonShardHeapUsage));
        expectThrows(AssertionError.class, () -> new NodeHeapEstimates(totalHeapUsage, hostedShardsHeapUsage, negative));
    }

    public void testNonShardHeapUsageIsTransportVersionGated() throws IOException {
        final long totalHeapUsage = randomNonNegativeLong();
        final long hostedShardsHeapUsage = randomNonNegativeLong();
        final long nonShardHeapUsage = randomLongBetween(1, 10_000_000);
        final var estimates = new NodeHeapEstimates(totalHeapUsage, hostedShardsHeapUsage, nonShardHeapUsage);

        final var currentVersionCopy = copyWriteable(estimates, writableRegistry(), NodeHeapEstimates::new, TransportVersion.current());
        assertThat(currentVersionCopy, equalTo(estimates));

        final var legacyVersion = TransportVersionUtils.getPreviousVersion(NodeHeapEstimates.NON_SHARD_HEAP_USAGE);
        final var legacyCopy = copyWriteable(estimates, writableRegistry(), NodeHeapEstimates::new, legacyVersion);
        assertThat(legacyCopy, equalTo(new NodeHeapEstimates(totalHeapUsage, hostedShardsHeapUsage, 0L)));
    }
}
