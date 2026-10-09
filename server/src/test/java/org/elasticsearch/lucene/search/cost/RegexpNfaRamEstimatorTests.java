/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
}
