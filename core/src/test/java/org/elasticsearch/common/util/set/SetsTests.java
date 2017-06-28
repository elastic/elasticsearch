/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.set;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        final int endExclusive = randomIntBetween(0, 256);
        final Tuple<Set<Integer>, Set<Integer>> sets = randomSets(endExclusive);
        final Set<Integer> difference = Sets.sortedDifference(sets.v1(), sets.v2());
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
            assertThat(difference.contains(i), equalTo(sets.v1().contains(i) && !sets.v2().contains(i)));
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
