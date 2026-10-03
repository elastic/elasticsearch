/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class CaseShardTests extends ESTestCase {

    /**
     * The property the whole design rests on: sharding partitions the cases. A case that lands in two
     * shards is counted twice and a case that lands in none is silently not run -- the second is the
     * failure mode that matters, because it is green.
     */
    public void testEveryCaseLandsInExactlyOneShard() {
        int count = randomIntBetween(1, 12);
        int cases = randomIntBetween(0, 500);
        Set<Integer> seen = new HashSet<>();
        for (int index = 1; index <= count; index++) {
            CaseShard shard = new CaseShard(index, count);
            for (int caseIndex = 0; caseIndex < cases; caseIndex++) {
                if (shard.selects(caseIndex)) {
                    assertTrue("case " + caseIndex + " selected by more than one of " + count + " shards", seen.add(caseIndex));
                }
            }
        }
        assertThat("every case must be selected by some shard", seen, hasSize(cases));
    }

    /** The reason for round-robin rather than contiguous blocks: no shard is more than one case bigger. */
    public void testShardsAreBalancedToWithinOneCase() {
        int count = randomIntBetween(2, 12);
        List<Integer> cases = casesNumbered(randomIntBetween(0, 500));
        int smallest = Integer.MAX_VALUE;
        int largest = 0;
        for (int index = 1; index <= count; index++) {
            int size = new CaseShard(index, count).select(cases).size();
            smallest = Math.min(smallest, size);
            largest = Math.max(largest, size);
        }
        assertThat(largest - smallest, lessThanOrEqualTo(1));
    }

    /** Selection preserves registration order, so a shard's failures reproduce in the order they ran. */
    public void testSelectionPreservesRegistrationOrder() {
        List<Integer> cases = casesNumbered(100);
        List<Integer> selected = new CaseShard(2, 3).select(cases);
        List<Integer> ascending = new ArrayList<>(selected);
        ascending.sort(null);
        assertThat(selected, equalTo(ascending));
        assertThat(selected.get(0), equalTo(1));
    }

    /**
     * The unsharded run returns the input list itself. Not merely equal: a copy would let a slicing bug
     * that only fires when nobody is sharding reorder or drop cases in the run everyone does locally.
     */
    public void testTheSingleShardReturnsTheCaseListItself() {
        List<Integer> cases = casesNumbered(20);
        assertThat(CaseShard.ALL.select(cases), sameInstance(cases));
        assertThat(CaseShard.parse("1/1"), equalTo(CaseShard.ALL));
    }

    public void testAnIndexOutsideTheShardCountIsRejected() {
        // Zero is the off-by-one a hand-written yml matrix produces, so it is rejected rather than read
        // as the first shard -- an accepted 0/6 would run shard 6's cases under shard 0's name.
        expectThrows(IllegalArgumentException.class, () -> CaseShard.parse("0/3"));
        expectThrows(IllegalArgumentException.class, () -> CaseShard.parse("4/3"));
        expectThrows(IllegalArgumentException.class, () -> CaseShard.parse("-1/3"));
        expectThrows(IllegalArgumentException.class, () -> CaseShard.parse("1/0"));
    }

    public void testAMalformedSpecIsRejected() {
        for (String spec : List.of("a/b", "3", "", "/", "1/2/3", "1/")) {
            expectThrows(IllegalArgumentException.class, "expected [" + spec + "] to be rejected", () -> CaseShard.parse(spec));
        }
        expectThrows(IllegalArgumentException.class, () -> CaseShard.parse(null));
    }

    public void testTheSpellingRoundTrips() {
        assertThat(CaseShard.parse(" 3 / 6 ").toString(), equalTo("3/6"));
        assertThat(CaseShard.parse("3/6"), equalTo(new CaseShard(3, 6)));
    }

    private static List<Integer> casesNumbered(int size) {
        List<Integer> cases = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            cases.add(i);
        }
        return cases;
    }
}
