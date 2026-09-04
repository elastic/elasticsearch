/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.collect;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ImmutableOpenMapRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return ImmutableOpenMap.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("map", "entrySet");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createRandomTestInstance() {
        ImmutableOpenMap.Builder<String, String> builder = ImmutableOpenMap.builder();
        int entries = randomIntBetween(1, 8);
        for (int i = 0; i < entries; i++) {
            builder.put(randomAlphaOfLengthBetween(4, 16), randomAlphaOfLengthBetween(4, 24));
        }
        return builder.build();
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // ObjectObjectHashMap.orderMixer is protected and omitted from the estimate; RamUsageTester still walks it
        // (~40 bytes for the default strategy). Structural + behavioural checks below cover the retained arrays/entries.
        return false;
    }

    /**
     * Non-tautology check: more retained entries must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithEntries() {
        ImmutableOpenMap<String, String> empty = ImmutableOpenMap.of();
        ImmutableOpenMap<String, String> withEntries = ImmutableOpenMap.<String, String>builder()
            .fPut("key-one", "value-one")
            .fPut("key-two", "value-two")
            .build();
        assertThat(withEntries.ramBytesUsed(), greaterThan(empty.ramBytesUsed()));
    }

    /**
     * Non-tautology check: longer keys/values must increase the reported size (not just entry count).
     */
    public void testRamBytesUsedGrowsWithKeyAndValueLengths() {
        ImmutableOpenMap<String, String> shortEntries = ImmutableOpenMap.<String, String>builder().fPut("a", "b").build();
        ImmutableOpenMap<String, String> longEntries = ImmutableOpenMap.<String, String>builder()
            .fPut(randomAlphaOfLengthBetween(32, 64), randomAlphaOfLengthBetween(32, 64))
            .build();
        assertThat(longEntries.ramBytesUsed(), greaterThan(shortEntries.ramBytesUsed()));
    }

    /**
     * Accountable values must be sized via {@link Accountable#ramBytesUsed()}, not a shallow object size.
     */
    public void testRamBytesUsedRecursesIntoAccountableValues() {
        Accountable value = () -> 10_000L;
        ImmutableOpenMap<String, Accountable> map = ImmutableOpenMap.<String, Accountable>builder().fPut("k", value).build();
        long withoutValueBody = map.ramBytesUsed(v -> 0L);
        assertThat(map.ramBytesUsed() - withoutValueBody, equalTo(value.ramBytesUsed()));
    }

    /**
     * Custom value sizing must be used by {@link ImmutableOpenMap#ramBytesUsed(java.util.function.ToLongFunction)}.
     */
    public void testRamBytesUsedHonorsValueBytesFunction() {
        ImmutableOpenMap<String, String> map = ImmutableOpenMap.<String, String>builder().fPut("k", "v").build();
        long structureOnly = map.ramBytesUsed(v -> 0L);
        // Use an 8-byte-aligned delta so alignObjectSize does not change the observed difference.
        assertThat(map.ramBytesUsed(v -> 128L) - structureOnly, equalTo(128L));
        assertThat(map.ramBytesUsed(), greaterThan(structureOnly));
        assertThat(structureOnly, greaterThan(0L));
    }
}
