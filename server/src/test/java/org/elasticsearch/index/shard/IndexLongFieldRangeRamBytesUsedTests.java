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
     * Non-tautology check: a range still tracking shards (holding a non-null {@code shards} array, e.g. {@code NO_SHARDS}) must be larger
     * than a complete range whose array is {@code null} (e.g. {@code UNKNOWN}).
     */
    public void testRamBytesUsedCountsTrackedShardsArray() {
        assertThat(IndexLongFieldRange.NO_SHARDS.isComplete(), org.hamcrest.Matchers.is(false));
        assertThat(IndexLongFieldRange.UNKNOWN.isComplete(), org.hamcrest.Matchers.is(true));
        assertThat(IndexLongFieldRange.NO_SHARDS.ramBytesUsed(), greaterThan(IndexLongFieldRange.UNKNOWN.ramBytesUsed()));
    }
}
