/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.set;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class SetsTests extends ESTestCase {

    public void testDifference() {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final Set<Integer> difference = Sets.difference(sets.v1(), sets.v2());
        assertDifference(endExclusive, sets, difference);
    }

    public void testSortedDifference() {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final SortedSet<Integer> difference = Sets.sortedDifference(sets.v1(), sets.v2());
        assertDifference(endExclusive, sets, difference);
        final Iterator<Integer> it = difference.iterator();
        if (it.hasNext()) {
            int current = it.next();
            while (it.hasNext()) {
                final int next = it.next();
                assertThat(next, greaterThan(current));
                current = next;
            }
        }
    }

    public void testIntersection() {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final Set<Integer> intersection = Sets.intersection(sets.v1(), sets.v2());
        assertThat(
            intersection,
            containsInAnyOrder(
                IntStream.range(0, endExclusive)
                    .boxed()
                    .filter(i -> (sets.v1().contains(i) && sets.v2().contains(i)))
                    .distinct()
                    .toArray(Integer[]::new)
            )
        );

        final Set<Integer> emptyIntersection = Sets.intersection(
            Sets.difference(sets.v1(), intersection),
            Sets.difference(sets.v2(), intersection)
        );
        assertThat(emptyIntersection.isEmpty(), is(true));
        // as an implementation detail, it's not just *some* empty set, but precisely *this* empty set
        assertThat(emptyIntersection, sameInstance(Set.of()));
    }

    public void testNewHashSetWithExpectedSize() {
        assertEquals(HashSet.class, Sets.newHashSetWithExpectedSize(randomIntBetween(0, 1_000_000)).getClass());
    }

    public void testNewLinkedHashSetWithExpectedSize() {
        assertEquals(LinkedHashSet.class, Sets.newLinkedHashSetWithExpectedSize(randomIntBetween(0, 1_000_000)).getClass());
    }

    public void testCapacityIsEnoughForSetToNotBeResized() {
        for (int i = 0; i < 1000; i++) {
            int size = randomIntBetween(0, 1_000_000);
            int capacity = Sets.capacity(size);
            assertThat(size, lessThanOrEqualTo((int) (capacity * 0.75f)));
        }
    }

    public void testAddToCopy() {
        assertThat(addToCopy(Set.of("a", "b"), "c"), containsInAnyOrder("a", "b", "c"));
        assertThat(addToCopy(Set.of("a", "b"), "c", "d"), containsInAnyOrder("a", "b", "c", "d"));
    }

    /**
     * Assert the difference between two sets is as expected.
     *
     * @param endExclusive the exclusive upper bound of the elements of either set
     * @param sets         a pair of sets with elements from {@code [0, endExclusive)}
     * @param difference   the difference between the two sets
     */
    private void assertDifference(final int endExclusive, final Tuple<Set<Integer>, Set<Integer>> sets, final Set<Integer> difference) {
        for (int i = 0; i < endExclusive; i++) {
            assertThat(difference.contains(i), equalTo(sets.v1().contains(i) && sets.v2().contains(i) == false));
        }
    }

    /**
     * Produces two random sets consisting of elements from {@code [0, endExclusive)}.
     *
     * @param endExclusive the exclusive upper bound of the elements of the sets
     * @return a pair of sets
     */
    private Tuple<Set<Integer>, Set<Integer>> randomSets(final int endExclusive) {
        final Set<Integer> left = new HashSet<>(randomSubsetOf(IntStream.range(0, endExclusive).boxed().collect(Collectors.toSet())));
        final Set<Integer> right = new HashSet<>(randomSubsetOf(IntStream.range(0, endExclusive).boxed().collect(Collectors.toSet())));
        return Tuple.tuple(left, right);
    }

}
