/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.swisshash.LongSwissHash;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Exercises {@link BlockHash.Router}, the bucket-sort-routing capability added for partitioned
 * hash aggregation. Confirms {@code partitionHashOfRow} agrees with {@link LongSwissHash#hash}
 * and that {@link BlockHash#router()} returns {@code null} when unsupported.
 */
public class LongBlockHashRouterTests extends BlockHashTestCase {

    private BlockHash longBlockHash() {
        return BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)), blockFactory, 16 * 1024, true);
    }

    public void testRouterNullWhenSwissHashUnavailable() {
        assumeFalse("only relevant when SwissHash is unavailable on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        try (BlockHash hash = longBlockHash()) {
            assertThat(hash.router(), nullValue());
        }
    }

    public void testPartitionHashMatchesLongSwissHash() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        long[] values = { 5, -3, 0, Long.MAX_VALUE, Long.MIN_VALUE, 12345 };
        try (BlockHash hash = longBlockHash(); LongBlock keys = blockFactory.newLongArrayVector(values, values.length).asBlock()) {
            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());
            Page page = new Page(keys);
            for (int i = 0; i < values.length; i++) {
                assertThat(router.partitionHashOfRow(page, i), equalTo(LongSwissHash.hash(values[i])));
            }
        }
    }
}
