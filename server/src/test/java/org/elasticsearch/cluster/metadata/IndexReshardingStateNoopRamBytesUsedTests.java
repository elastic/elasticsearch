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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexReshardingStateNoopRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexReshardingState.Noop.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return new IndexReshardingState.Noop();
    }

    /**
     * Non-tautology check: {@link IndexReshardingState.Noop} has no reference fields; the estimate is the shallow instance size.
     */
    public void testRamBytesUsedIsShallowSize() {
        IndexReshardingState.Noop noop = new IndexReshardingState.Noop();
        assertThat(noop.ramBytesUsed(), equalTo(RamUsageEstimator.shallowSizeOfInstance(IndexReshardingState.Noop.class)));
    }
}
