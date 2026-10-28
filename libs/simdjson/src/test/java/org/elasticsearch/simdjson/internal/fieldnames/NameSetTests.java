/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable.Child.NameSet;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytes;

// Whitebox tests for FrozenFieldNameTable.Child.NameSet, the flat parallel-array store shared by
// Child's pre-freeze learning phase (NameSet.growable()) and post-freeze overflow buffer
// (NameSet.capped(n)). FrozenFieldNameTableTests covers Child/FrozenFieldNameTable end-to-end
// behavior; these tests target the array bookkeeping directly, which is awkward to pin down
// precisely (exact drop patterns, doubling boundaries) by driving it only through Child's misses.
public class NameSetTests extends ESTestCase {

    // ---- Basic lookup/add ----

    // An empty set, growable or capped, must reject every lookup without touching its (null)
    // backing arrays.
    public void testLookupOnEmptySetReturnsNull() {
        assertNull(lookup(NameSet.growable(), "anything"));
        assertNull(lookup(NameSet.capped(4), "anything"));
        assertNull(lookup(NameSet.capped(randomIntBetween(1, 16)), randomAlphaOfLengthBetween(1, 20)));
    }

    // add() followed by lookup() with the same bytes/hash must return the exact name instance
    // that was added, for both a growable and a capped set.
    public void testAddThenLookupReturnsSameInstance() {
        for (NameSet set : List.of(NameSet.growable(), NameSet.capped(4))) {
            String name = randomAlphaOfLengthBetween(1, 20);
            String added = add(set, name);
            assertSame("lookup must return the exact instance add() was given", added, lookup(set, name));
        }
    }

    // A name never added must not be found among others that were.
    public void testLookupMissesAnUnaddedName() {
        for (NameSet set : List.of(NameSet.growable(), NameSet.capped(4))) {
            add(set, "present");
            assertNull(lookup(set, "absent"));
        }
    }

    // ---- Hash-based disambiguation ----

    // add()/lookup() take the hash as a caller-supplied value rather than computing it, so two
    // entries can deliberately share a hash without needing a real collision: lookup must fall
    // through to the full byte comparison and never confuse them.
    public void testLookupDistinguishesEntriesWithTheSameHash() {
        NameSet set = NameSet.growable();
        int sharedHash = 42;
        byte[] bufA = toBytes("alpha");
        byte[] bufB = toBytes("bravo");
        set.add("alpha", bufA, 0, bufA.length, sharedHash);
        set.add("bravo", bufB, 0, bufB.length, sharedHash);

        assertEquals("alpha", set.lookup(bufA, 0, bufA.length, sharedHash));
        assertEquals("bravo", set.lookup(bufB, 0, bufB.length, sharedHash));
    }

    // ---- Growable: unbounded, doubles on demand ----

    // Every entry added must remain resolvable across repeated doublings (128 -> 256 -> 512),
    // including right at the boundary. This also confirms a growable set never rejects an add,
    // however many entries it already holds: a dropped add would surface here as a count or
    // lookup mismatch.
    public void testGrowableSurvivesRepeatedDoubling() {
        NameSet set = NameSet.growable();
        List<String> names = randomDistinctFieldNames(randomIntBetween(300, 600));
        for (String name : names) {
            add(set, name);
        }
        assertEquals(names.size(), set.count());
        for (String name : names) {
            assertEquals("every added name must resolve after growing: " + name, name, lookup(set, name));
        }
    }

    // ---- Capped: bounded, rejects past capacity ----

    // A capped set accepts exactly up to its capacity and silently drops the rest: dropped names
    // are not recorded (absent from lookup) but the count never exceeds the cap.
    public void testCappedAcceptsUpToCapacityThenDrops() {
        int capacity = randomIntBetween(2, 10);
        NameSet set = NameSet.capped(capacity);
        List<String> names = randomDistinctFieldNames(capacity + randomIntBetween(1, 10));
        for (String name : names) {
            add(set, name);
        }
        assertEquals("count must never exceed the cap", capacity, set.count());
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (i < capacity) {
                assertEquals("name within the cap must resolve: " + name, name, lookup(set, name));
            } else {
                assertNull("name past the cap must not be recorded: " + name, lookup(set, name));
            }
        }
    }

    // ---- Compaction ----

    // retainIf must keep exactly the entries its predicate selects - dropped at the start,
    // middle, end, none, or all - and repack survivors so they still resolve, regardless of
    // where in the original set they were dropped from.
    public void testRetainIfCompactsToArbitrarySubsets() {
        int iterations = atLeast(20);
        for (int itr = 0; itr < iterations; itr++) {
            int total = randomIntBetween(1, 12);
            List<String> names = randomDistinctFieldNames(total);
            List<byte[]> bufs = new ArrayList<>(total);
            List<Integer> hashes = new ArrayList<>(total);

            for (NameSet set : List.of(NameSet.growable(), NameSet.capped(total))) {
                for (String name : names) {
                    byte[] buf = toBytes(name);
                    int hash = FieldNameHash.hashName(buf, 0, buf.length);
                    set.add(name, buf, 0, buf.length, hash);
                    bufs.add(buf);
                    hashes.add(hash);
                }

                boolean[] keep = new boolean[total];
                for (int i = 0; i < total; i++) {
                    keep[i] = randomBoolean();
                }
                set.retainIf(i -> keep[i]);

                int expectedKept = 0;
                for (int i = 0; i < total; i++) {
                    byte[] buf = bufs.get(i);
                    int hash = hashes.get(i);
                    if (keep[i]) {
                        expectedKept++;
                        assertEquals("kept entry must still resolve: " + names.get(i), names.get(i), set.lookup(buf, 0, buf.length, hash));
                    } else {
                        assertNull("dropped entry must no longer resolve: " + names.get(i), set.lookup(buf, 0, buf.length, hash));
                    }
                }
                assertEquals("count must match the number of retained entries", expectedKept, set.count());
            }
        }
    }

    // After retainIf compacts a set, further adds must append correctly from the reduced count,
    // without disturbing the entries retainIf kept.
    public void testAddAfterRetainIfContinuesFromCompactedCount() {
        for (NameSet set : List.of(NameSet.growable(), NameSet.capped(randomIntBetween(3, 16)))) {
            add(set, "a");
            add(set, "b");
            add(set, "c");

            set.retainIf(i -> i != 1); // drop "b", keep "a" and "c"
            assertEquals(2, set.count());

            add(set, "d");
            assertEquals(3, set.count());

            assertEquals("a", lookup(set, "a"));
            assertNull(lookup(set, "b"));
            assertEquals("c", lookup(set, "c"));
            assertEquals("d", lookup(set, "d"));
        }
    }

    private static List<String> randomDistinctFieldNames(int count) {
        Set<String> unique = new HashSet<>();
        while (unique.size() < count) {
            unique.add(randomAlphaOfLengthBetween(0, 24) + "_" + unique.size());
        }
        return List.copyOf(unique);
    }

    private static String add(NameSet set, String name) {
        byte[] buf = toBytes(name);
        int hash = FieldNameHash.hashName(buf, 0, buf.length);
        set.add(name, buf, 0, buf.length, hash);
        return name;
    }

    private static String lookup(NameSet set, String name) {
        byte[] buf = toBytes(name);
        int hash = FieldNameHash.hashName(buf, 0, buf.length);
        return set.lookup(buf, 0, buf.length, hash);
    }
}
