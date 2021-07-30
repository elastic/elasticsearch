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
import java.util.Set;
import java.util.SortedSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SetsTests extends ESTestCase {

    public void testDifference() {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final Set<Integer> difference = Sets.difference(sets.v1(), sets.v2());
        assertDifference(endExclusive, sets, difference);
    }

    public void testSortedDifference() {
        runSortedDifferenceTest(Sets::sortedDifference, set -> {});
    }

    public void testUnmodifiableSortedDifference() {
        runSortedDifferenceTest(
                // assert the resulting difference us unmodifiable
                Sets::unmodifiableSortedDifference, set -> expectThrows(UnsupportedOperationException.class, () -> set.add(randomInt())));
    }

    private void runSortedDifferenceTest(
        final BiFunction<Set<Integer>, Set<Integer>, SortedSet<Integer>> sortedDifference,
        final Consumer<Set<Integer>> asserter) {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final SortedSet<Integer> difference = sortedDifference.apply(sets.v1(), sets.v2());
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
        asserter.accept(difference);
    }

    public void testIntersection() {
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final Set<Integer> intersection = Sets.intersection(sets.v1(), sets.v2());
        final Set<Integer> expectedIntersection = IntStream.range(0, endExclusive)
                .boxed()
                .filter(i -> (sets.v1().contains(i) && sets.v2().contains(i)))
                .collect(Collectors.toSet());
        assertThat(intersection, containsInAnyOrder(expectedIntersection.toArray(new Integer[0])));
    }

    /**
     * Assert the difference between two sets is as expected.
     *
     * @param endExclusive the exclusive upper bound of the elements of either set
     * @param sets         a pair of sets with elements from {@code [0, endExclusive)}
     * @param difference   the difference between the two sets
     */
    private void assertDifference(
            final int endExclusive, final Tuple<Set<Integer>, Set<Integer>> sets, final Set<Integer> difference) {
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
