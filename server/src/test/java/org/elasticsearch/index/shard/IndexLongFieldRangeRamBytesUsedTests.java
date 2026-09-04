/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IndexLongFieldRangeRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexLongFieldRange.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("shards");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        // Mix of null shards (complete/empty) and non-null arrays of varying length, so a missed array-length term fails.
        return IndexLongFieldRangeTestUtils.randomSpecificRange();
    }

    /**
     * Shared sentinels are reused across indices; their body must not contribute to per-reference estimates.
     */
    public void testRamBytesUsedExcludesSharedSentinels() {
        assertThat(IndexLongFieldRange.NO_SHARDS.ramBytesUsed(), equalTo(0L));
        assertThat(IndexLongFieldRange.EMPTY.ramBytesUsed(), equalTo(0L));
        assertThat(IndexLongFieldRange.UNKNOWN.ramBytesUsed(), equalTo(0L));
    }

    /**
     * Non-tautology check: a non-sentinel range still tracking shards must report a positive size larger than a shared sentinel.
     */
    public void testRamBytesUsedCountsTrackedShardsArray() {
        IndexLongFieldRange tracking = IndexLongFieldRange.NO_SHARDS.extendWithShardRange(0, 2, ShardLongFieldRange.of(1L, 10L));
        assertThat(tracking.isComplete(), org.hamcrest.Matchers.is(false));
        assertThat(tracking.ramBytesUsed(), greaterThan(0L));
        assertThat(tracking.ramBytesUsed(), greaterThan(IndexLongFieldRange.NO_SHARDS.ramBytesUsed()));
    }
}
