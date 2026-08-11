/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.node;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class DiscoveryNodeFiltersRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return DiscoveryNodeFilters.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("filters", "withoutTierPreferences", "opType");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        return DiscoveryNodeFilters.buildFromKeyValues(
            DiscoveryNodeFilters.OpType.AND,
            Map.of("_id", List.of("n1", "n2"), "rack", List.of("r1"))
        );
    }

    /**
     * Non-tautology check: more filter attributes must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithFilters() {
        DiscoveryNodeFilters one = DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.AND, Map.of("_id", List.of("n1")));
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(one.ramBytesUsed()));
    }
}
