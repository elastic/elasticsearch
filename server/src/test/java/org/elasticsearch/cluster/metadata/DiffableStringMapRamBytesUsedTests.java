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

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class DiffableStringMapRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return DiffableStringMap.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("innerMap");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return new DiffableStringMap(Map.of("key", randomAlphaOfLengthBetween(8, 64), "other", randomAlphaOfLengthBetween(4, 32)));
    }

    /**
     * Non-tautology check: a longer retained string value must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithValueLength() {
        DiffableStringMap small = new DiffableStringMap(Map.of("key", "a"));
        DiffableStringMap large = new DiffableStringMap(Map.of("key", "x".repeat(256)));
        assertThat(large.ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
