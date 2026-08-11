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

public class IndexReshardingMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexReshardingMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("state");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return IndexReshardingMetadata.newSplitByMultiple(8, 2);
    }

    /**
     * Non-tautology check: a split into more shards holds larger source/target arrays and so reports a larger estimate.
     */
    public void testRamBytesUsedGrowsWithShardCount() {
        IndexReshardingMetadata small = IndexReshardingMetadata.newSplitByMultiple(1, 2);
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
