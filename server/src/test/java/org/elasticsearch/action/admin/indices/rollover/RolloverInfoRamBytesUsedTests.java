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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class RolloverInfoRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return RolloverInfo.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("alias", "metConditions");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Nested Condition instances omit shared Type enum singletons that RamUsageTester includes.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        int n = randomIntBetween(1, 4);
        List<Condition<?>> conditions = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            conditions.add(new MaxDocsCondition(randomNonNegativeLong()));
        }
        return new RolloverInfo(randomAlphaOfLengthBetween(1, 12), conditions, randomNonNegativeLong());
    }

    /**
     * Non-tautology check: adding more met conditions must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithConditions() {
        RolloverInfo one = new RolloverInfo("alias", List.of(new MaxDocsCondition(1L)), 1L);
        RolloverInfo many = new RolloverInfo(
            "alias",
            List.of(new MaxDocsCondition(1L), new MaxDocsCondition(2L), new MaxDocsCondition(3L)),
            1L
        );
        assertThat(many.ramBytesUsed(), greaterThan(one.ramBytesUsed()));
    }
}
