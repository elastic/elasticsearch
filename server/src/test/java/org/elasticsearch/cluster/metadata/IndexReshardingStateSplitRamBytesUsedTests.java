/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class IndexReshardingStateSplitRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexReshardingState.Split.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("sourceShards", "targetShards");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Array elements are shared enum singletons; RamUsageTester includes them and ramBytesUsed() does not.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return IndexReshardingState.Split.newSplitByMultiple(randomIntBetween(1, 16), 2);
    }

    /**
     * Non-tautology check: more source shards means larger arrays and a larger estimate.
     */
    public void testRamBytesUsedGrowsWithShardCount() {
        IndexReshardingState.Split small = IndexReshardingState.Split.newSplitByMultiple(1, 2);
        IndexReshardingState.Split large = IndexReshardingState.Split.newSplitByMultiple(8, 2);
        assertThat(large.ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
