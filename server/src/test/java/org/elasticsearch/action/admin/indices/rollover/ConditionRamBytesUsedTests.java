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
        return Set.of("name", "value");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // Shared enum singleton; only the field reference is counted in shallowSizeOf(this).
        return Set.of("type");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // RamUsageTester includes the Type enum instance; ramBytesUsed() does not, by design.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return switch (randomIntBetween(0, 10)) {
            case 0 -> new MaxDocsCondition(randomNonNegativeLong());
            case 1 -> new MinDocsCondition(randomNonNegativeLong());
            case 2 -> new MaxPrimaryShardDocsCondition(randomNonNegativeLong());
            case 3 -> new MinPrimaryShardDocsCondition(randomNonNegativeLong());
            case 4 -> new MaxSizeCondition(randomByteSizeValue());
            case 5 -> new MinSizeCondition(randomByteSizeValue());
            case 6 -> new MaxPrimaryShardSizeCondition(randomByteSizeValue());
            case 7 -> new MinPrimaryShardSizeCondition(randomByteSizeValue());
            case 8 -> new MaxAgeCondition(randomTimeValue());
            case 9 -> new MinAgeCondition(randomTimeValue());
            case 10 -> new OptimalShardCountCondition(randomIntBetween(1, 100));
            default -> throw new AssertionError("unexpected condition branch");
        };
    }

    /**
     * Non-tautology check: the condition value (Long, ByteSizeValue, TimeValue, or Integer) must contribute a cost on top of the shallow
     * instance size.
     */
    public void testRamBytesUsedCountsConditionValue() {
        for (int i = 0; i < 10; i++) {
            Condition<?> instance = (Condition<?>) createRandomTestInstance();
            assertThat(instance.ramBytesUsed(), greaterThan(shallowSizeOf(instance)));
        }
    }
}
