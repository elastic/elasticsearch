/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class ConditionRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return Condition.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("name", "value", "type");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return new MaxDocsCondition(1L);
    }

    /**
     * Non-tautology check: the different value types must each contribute a boxed/value cost on top of the shallow condition size.
     */
    public void testRamBytesUsedCountsConditionValue() {
        long shallow = shallowSizeOf(new MaxDocsCondition(1L));
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(shallow));
        assertThat(new MaxSizeCondition(ByteSizeValue.ofMb(1)).ramBytesUsed(), greaterThan(shallow));
        assertThat(new MaxAgeCondition(TimeValue.timeValueDays(1)).ramBytesUsed(), greaterThan(shallow));
    }
}
