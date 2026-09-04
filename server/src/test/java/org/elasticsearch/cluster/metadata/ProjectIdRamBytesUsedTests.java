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

public class ProjectIdRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return ProjectId.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("id");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return ProjectId.fromId(randomAlphaOfLengthBetween(8, 32));
    }

    /**
     * Non-tautology check: a longer project id string must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithIdLength() {
        ProjectId small = ProjectId.fromId("a");
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
