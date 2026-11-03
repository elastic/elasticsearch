/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class MetadataValuesTests extends ComputeTestCase {

    public void testStringReturnsFirstInstanceAndChargesOnce() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            String first = copyOf("host.name");
            assertThat(values.string(first), sameInstance(first));
            long charged = breaker.getUsed();
            assertThat(values.string(copyOf("host.name")), sameInstance(first));
            assertThat(breaker.getUsed(), equalTo(charged));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSharedListReturnsOneInstancePerDistinctValues() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> first = values.sharedList(new TreeSet<>(Set.of("percent", "ratio")));
            long charged = breaker.getUsed();
            assertThat(values.sharedList(new TreeSet<>(Set.of("ratio", "percent"))), sameInstance(first));
            assertThat(first, equalTo(List.of("percent", "ratio")));
            assertThat(breaker.getUsed(), equalTo(charged));
        }
    }

    public void testSharedListHoldsCanonicalStrings() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            String unit = values.string(copyOf("percent"));
            List<String> list = values.sharedList(new TreeSet<>(Set.of(copyOf("percent"))));
            assertThat(list.getFirst(), sameInstance(unit));
        }
    }

    public void testSharedUnionOfSubsetKeepsCurrentList() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> current = values.sharedList(new TreeSet<>(Set.of("a", "b")));
            List<String> subset = values.sharedList(new TreeSet<>(Set.of("a")));
            assertThat(values.sharedUnion(current, subset), sameInstance(current));
        }
    }

    public void testSharedWithNullValueKeepsCurrentList() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> current = values.sharedList(new TreeSet<>(Set.of("a")));
            assertThat(values.sharedWithValue(current, null), sameInstance(current));
        }
    }

    public void testUnsharedUnionKeepsListWhenNothingIsNew() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> current = values.unsharedUnion(List.of(), Set.of("a", "b"));
            long charged = breaker.getUsed();
            assertThat(values.unsharedUnion(current, Set.of("b")), sameInstance(current));
            assertThat(breaker.getUsed(), equalTo(charged));
        }
    }

    /** A list that grows one key at a time must not keep the charge of every earlier version. */
    public void testUnsharedUnionReleasesReplacedList() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues growing = new MetadataValues(breaker, "test")) {
            List<String> keys = List.of();
            for (int i = 0; i < 256; i++) {
                keys = growing.unsharedUnion(keys, Set.of("key" + i));
            }
            long chargedForGrowth = breaker.getUsed();
            try (MetadataValues direct = new MetadataValues(breaker, "test")) {
                TreeSet<String> all = new TreeSet<>();
                for (int i = 0; i < 256; i++) {
                    all.add("key" + i);
                }
                direct.unsharedUnion(List.of(), all);
                assertThat(breaker.getUsed() - chargedForGrowth, equalTo(chargedForGrowth));
            }
        }
    }

    /** Releasing a shared list would leave the other entries that hold it uncharged. */
    public void testUnsharedUnionKeepsChargeOfSharedList() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> shared = values.sharedList(new TreeSet<>(Set.of("a")));
            long chargedForShared = breaker.getUsed();
            List<String> grown = values.unsharedUnion(shared, Set.of("b"));
            assertThat(grown, equalTo(List.of("a", "b")));
            assertThat(breaker.getUsed(), greaterThan(chargedForShared));
        }
    }

    public void testUnsharedUnionIsSorted() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            List<String> keys = values.unsharedUnion(List.of("b", "d"), Set.of("c", "a", "e"));
            assertThat(keys, equalTo(List.of("a", "b", "c", "d", "e")));
        }
    }

    public void testCloseReleasesAllCharges() {
        CircuitBreaker breaker = blockFactory().breaker();
        MetadataValues values = new MetadataValues(breaker, "test");
        values.string("host");
        values.sharedList(new TreeSet<>(Set.of("percent")));
        values.unsharedUnion(List.of(), Set.of("a", "b"));
        assertThat(breaker.getUsed(), greaterThan(0L));
        values.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testEmptySharedListIsFree() {
        CircuitBreaker breaker = blockFactory().breaker();
        try (MetadataValues values = new MetadataValues(breaker, "test")) {
            assertThat(values.sharedList(new TreeSet<>()), equalTo(List.of()));
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    /** Returns an equal string that is a different instance, as strings read from separate pages are. */
    private static String copyOf(String value) {
        return new StringBuilder(value).toString();
    }
}
