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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

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
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return new RolloverInfo(
            "alias",
            List.of(new MaxDocsCondition(1L), new MaxAgeCondition(TimeValue.timeValueDays(1)), new MaxDocsCondition(2L)),
            1L
        );
    }

    /**
     * Non-tautology check: adding more met conditions must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithConditions() {
        RolloverInfo one = new RolloverInfo("alias", List.of(new MaxDocsCondition(1L)), 1L);
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(one.ramBytesUsed()));
    }
}
