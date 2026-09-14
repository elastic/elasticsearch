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

import java.util.HashMap;
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
        return Set.of("filters", "withoutTierPreferences");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // Shared enum singleton; only the field reference is counted in BASE_RAM_BYTES_USED.
        return Set.of("opType");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // RamUsageTester includes the OpType enum instance; ramBytesUsed() does not, by design.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        Map<String, List<String>> filters = new HashMap<>();
        if (randomBoolean()) {
            int attrs = randomIntBetween(1, 4);
            for (int i = 0; i < attrs; i++) {
                filters.put("attr" + i, randomList(1, 3, () -> randomAlphaOfLengthBetween(1, 8)));
            }
        }
        if (filters.isEmpty() || randomBoolean()) {
            filters.put("_tier_preference", List.of(randomFrom("data_hot", "data_warm", "data_cold")));
        }
        return DiscoveryNodeFilters.buildFromKeyValues(randomFrom(DiscoveryNodeFilters.OpType.values()), filters);
    }

    /**
     * Non-tautology check: more filter attributes must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithFilters() {
        DiscoveryNodeFilters one = DiscoveryNodeFilters.buildFromKeyValues(DiscoveryNodeFilters.OpType.AND, Map.of("_id", List.of("n1")));
        DiscoveryNodeFilters many = DiscoveryNodeFilters.buildFromKeyValues(
            DiscoveryNodeFilters.OpType.AND,
            Map.of("_id", List.of("n1", "n2"), "rack", List.of("r1"))
        );
        assertThat(many.ramBytesUsed(), greaterThan(one.ramBytesUsed()));
    }

    /**
     * A {@code _tier_preference} filter materializes a distinct {@code withoutTierPreferences} instance whose heap is added on top.
     */
    public void testRamBytesUsedCountsWithoutTierPreferences() {
        DiscoveryNodeFilters withoutTier = DiscoveryNodeFilters.buildFromKeyValues(
            DiscoveryNodeFilters.OpType.AND,
            Map.of("_id", List.of("n1"))
        );
        DiscoveryNodeFilters withTier = DiscoveryNodeFilters.buildFromKeyValues(
            DiscoveryNodeFilters.OpType.AND,
            Map.of("_id", List.of("n1"), "_tier_preference", List.of("data_hot"))
        );
        assertThat(withTier.ramBytesUsed(), greaterThan(withoutTier.ramBytesUsed()));
    }
}
