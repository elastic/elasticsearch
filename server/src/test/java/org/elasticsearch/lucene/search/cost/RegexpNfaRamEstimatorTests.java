/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RegexpNfaRamEstimatorTests extends ESTestCase {

    public void testIsPositiveForSimplePatterns() {
        assertThat(RegexpNfaRamEstimator.estimateRamBytes("", RegExp.ALL, 0), greaterThan(0L));
        assertThat(RegexpNfaRamEstimator.estimateRamBytes("abc", RegExp.ALL, 0), greaterThan(0L));
    }

    public void testGrowsWithRepetition() {
        long small = RegexpNfaRamEstimator.estimateRamBytes("a{10}", RegExp.ALL, 0);
        long large = RegexpNfaRamEstimator.estimateRamBytes("a{100000}", RegExp.ALL, 0);
        assertThat(large, greaterThan(small));
    }

    public void testSaturatesForPathologicalPattern() {
        assertThat(RegexpNfaRamEstimator.estimateRamBytes("a{100000000}", RegExp.ALL, 0), greaterThan(ByteSizeValue.ofGb(5).getBytes()));
        assertThat(RegexpNfaRamEstimator.estimateRamBytes("(a{2000000000}){2000000000}", RegExp.ALL, 0), equalTo(Long.MAX_VALUE));
    }

    public void testIgnoresLiteralBraces() {
        long unescaped = RegexpNfaRamEstimator.estimateRamBytes("a{100000000}", RegExp.ALL, 0);
        long escaped = RegexpNfaRamEstimator.estimateRamBytes("a\\{100000000\\}", RegExp.ALL, 0);
        assertThat(escaped, greaterThan(0L));
        assertThat(unescaped, greaterThan(escaped));
    }

    /**
     * The estimate must bound what Lucene actually builds, including the transitions that concatenating a sub-automaton
     * which accepts the empty string adds between every earlier accept state and every later initial transition.
     */
    public void testBoundsTheBuiltAutomaton() {
        String[] patterns = {
            "",
            "abc",
            ".*foo.*",
            "[a-z]{1,500}",
            "(a|b|c){0,100}",
            "[^a]",
            "~(abc)",
            "a&b",
            "<0-999999999>",
            "<0-999999999>{100}",
            "(.*){100}{10}",
            "(a*){30}{30}",
            "(.?){200}",
            "(a|b*)+",
            "((a|b*)+){50}",
            "a{1000}",
            "(ab|cd){1,200}",
            "\\[ab\\]{100}",
            "[ab]{1000}{5}",
            "(a|b)*a(a|b){5}",
            "a{3,}",
            "(ab*){2,7}c?" };
        for (String pattern : patterns) {
            RegExp re = new RegExp(pattern, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT);
            Automaton built = re.toAutomaton();
            long estimate = RegexpNfaRamEstimator.estimateRamBytes(re);
            assertThat(pattern, estimate, greaterThanOrEqualTo(built.ramBytesUsed()));
            assertThat(pattern, estimate, greaterThanOrEqualTo(built.getNumTransitions() * RegexpNfaRamEstimator.BYTES_PER_TRANSITION));
        }
    }

    /** Fourteen characters, ten thousand states and fifty million transitions: a state count alone misses it by 600x. */
    public void testNullableRepeatIsQuadratic() {
        assertThat(
            RegexpNfaRamEstimator.estimateRamBytes("(.*){100}{100}", RegExp.ALL, 0),
            greaterThan(50_005_000L * RegexpNfaRamEstimator.BYTES_PER_TRANSITION)
        );
        assertThat(RegexpNfaRamEstimator.estimateRamBytes("(a*){60}{60}", RegExp.ALL, 0), greaterThan(ByteSizeValue.ofMb(64).getBytes()));
    }

    public void testWildcardEstimateBoundsTheBuiltAutomaton() {
        for (String wildcard : new String[] { "", "abc", "a*b?c", "*a*", "\\*lit\\?", "*".repeat(200), "?".repeat(300) + "*" }) {
            Automaton built = org.apache.lucene.search.WildcardQuery.toAutomaton(
                new org.apache.lucene.index.Term("f", wildcard),
                org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            );
            assertThat(wildcard, RegexpNfaRamEstimator.estimateWildcardRamBytes(wildcard), greaterThanOrEqualTo(built.ramBytesUsed()));
        }
        assertThat(RegexpNfaRamEstimator.estimateWildcardRamBytes("*".repeat(1000)), greaterThan(ByteSizeValue.ofMb(10).getBytes()));
    }
}
