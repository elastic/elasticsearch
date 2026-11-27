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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import static org.hamcrest.Matchers.equalTo;

public class ShardAndIndexHeapUsageTests extends AbstractWireSerializingTestCase<ShardAndIndexHeapUsage> {

    @Override
    protected Writeable.Reader<ShardAndIndexHeapUsage> instanceReader() {
        return ShardAndIndexHeapUsage::new;
    }

    @Override
    protected ShardAndIndexHeapUsage createTestInstance() {
        return randomShardAndIndexHeapUsage();
    }

    @Override
    protected ShardAndIndexHeapUsage mutateInstance(ShardAndIndexHeapUsage instance) {
        long shardHeapUsageBytes = instance.shardHeapUsageBytes();
        long indexHeapUsageBytes = instance.indexHeapUsageBytes();
        long postingsHeapUsageBytes = instance.postingsHeapUsageBytes();
        return switch (between(0, 2)) {
            case 0 -> {
                if (postingsHeapUsageBytes == Long.MAX_VALUE) {
                    // postings <= shard forces the shard size to Long.MAX_VALUE as well, so this branch has no
                    // other legal shard size. Change the index size instead of asking a supplier for a new one.
                    final long newIndex = indexHeapUsageBytes == Long.MAX_VALUE ? 0L : indexHeapUsageBytes + 1L;
                    yield new ShardAndIndexHeapUsage(shardHeapUsageBytes, newIndex, postingsHeapUsageBytes);
                }
                // Keep postings fixed; pick a different shard size that still satisfies postings <= shard.
                final long newShard = randomValueOtherThan(
                    shardHeapUsageBytes,
                    () -> randomLongBetween(postingsHeapUsageBytes, Long.MAX_VALUE)
                );
                yield new ShardAndIndexHeapUsage(newShard, indexHeapUsageBytes, postingsHeapUsageBytes);
            }
            case 1 -> {
                final long newIndex = randomValueOtherThan(indexHeapUsageBytes, ESTestCase::randomNonNegativeLong);
                yield new ShardAndIndexHeapUsage(shardHeapUsageBytes, newIndex, postingsHeapUsageBytes);
            }
            case 2 -> {
                if (shardHeapUsageBytes == 0L) {
                    // postings must stay 0 when shard is 0; change shard alone instead
                    yield new ShardAndIndexHeapUsage(
                        randomValueOtherThan(0L, () -> randomLongBetween(1, Long.MAX_VALUE)),
                        indexHeapUsageBytes,
                        0L
                    );
                }
                final long maxPostings = shardHeapUsageBytes;
                final long newPostings = randomValueOtherThan(postingsHeapUsageBytes, () -> randomLongBetween(0, maxPostings));
                yield new ShardAndIndexHeapUsage(shardHeapUsageBytes, indexHeapUsageBytes, newPostings);
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        };
    }

    public void testPostingsHeapUsageIsTransportVersionGated() throws Exception {
        final long shardHeapUsageBytes = randomLongBetween(1, Long.MAX_VALUE);
        final long postingsHeapUsageBytes = randomLongBetween(1, shardHeapUsageBytes);
        final var usage = new ShardAndIndexHeapUsage(shardHeapUsageBytes, randomNonNegativeLong(), postingsHeapUsageBytes);

        final var currentVersionCopy = copyInstance(usage, TransportVersion.current());
        assertThat(currentVersionCopy.shardHeapUsageBytes(), equalTo(shardHeapUsageBytes));
        assertThat(currentVersionCopy.indexHeapUsageBytes(), equalTo(usage.indexHeapUsageBytes()));
        assertThat(currentVersionCopy.postingsHeapUsageBytes(), equalTo(postingsHeapUsageBytes));

        final var preVersion = TransportVersionUtils.getPreviousVersion(ShardAndIndexHeapUsage.INCLUDE_POSTINGS_IN_SHARD_AND_INDEX_HEAP);
        final var preVersionCopy = copyInstance(usage, preVersion);
        assertThat(preVersionCopy.shardHeapUsageBytes(), equalTo(shardHeapUsageBytes));
        assertThat(preVersionCopy.indexHeapUsageBytes(), equalTo(usage.indexHeapUsageBytes()));
        assertThat(preVersionCopy.postingsHeapUsageBytes(), equalTo(0L));
    }

    public void testShardHeapUsageBytesExcludingPostings() {
        final var shardHeapUsageBytes = randomNonNegativeLong();
        final var postingsHeapUsageBytes = randomLongBetween(0, shardHeapUsageBytes);
        final var usage = new ShardAndIndexHeapUsage(shardHeapUsageBytes, randomNonNegativeLong(), postingsHeapUsageBytes);
        assertThat(usage.shardHeapUsageBytesExcludingPostings(), equalTo(shardHeapUsageBytes - postingsHeapUsageBytes));
    }

    private static ShardAndIndexHeapUsage randomShardAndIndexHeapUsage() {
        final long shardHeapUsageBytes = randomNonNegativeLong();
        final long postingsHeapUsageBytes = randomLongBetween(0, shardHeapUsageBytes);
        return new ShardAndIndexHeapUsage(shardHeapUsageBytes, randomNonNegativeLong(), postingsHeapUsageBytes);
    }
}
