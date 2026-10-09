/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

public class SummaryPolicyTests extends ESTestCase {

    public void testEnabledFollowsTheByteBound() {
        assertTrue(SummaryPolicy.sized(1).enabled());
        assertFalse(SummaryPolicy.sized(0).enabled());
        assertFalse(SummaryPolicy.NONE.enabled());
    }

    public void testRejectsANegativeBound() {
        expectThrows(IllegalArgumentException.class, () -> SummaryPolicy.sized(-1));
    }

    // NOTE: the two match by default so a flush and the merge that reads it are bounded alike. A wider
    // summary is not useless, since what a merge may hold is a multiple of the larger of the two: widening
    // it lets the merge count more terms and tighten its bound.
    public void testDefaultSummaryAndDictionaryCapsMatch() {
        assertEquals(StringColumnOptions.DEFAULT_DICTIONARY.maxBytes(), StringColumnOptions.DEFAULT_SUMMARY.maxBytes());
    }

    public void testRejectsAMultipleBelowOne() {
        expectThrows(IllegalArgumentException.class, () -> new SummaryPolicy(1024, 0));
    }

    // NOTE: a column remembers at least what a dictionary could name on it, whatever it is allowed to leave
    // behind, or a policy that summarises nothing would stop it recognising a dictionary worth building.
    public void testAVocabularyIsBoundedByWhicheverCapIsLarger() {
        final DictionaryPolicy dictionaryPolicy = new DictionaryPolicy(1024, 0.5, 0.2);

        assertEquals(1024, SummaryPolicy.sized(256).surveyBudgetBytes(dictionaryPolicy));
        assertEquals(1024, SummaryPolicy.NONE.surveyBudgetBytes(dictionaryPolicy));
        assertEquals(4096, SummaryPolicy.sized(4096).surveyBudgetBytes(dictionaryPolicy));
    }

    public void testAMergeHoldsTheMultipleOfThat() {
        final DictionaryPolicy dictionaryPolicy = new DictionaryPolicy(1024, 0.5, 0.2);

        assertEquals(4096, SummaryPolicy.sized(256).mergeBudgetBytes(dictionaryPolicy));
        assertEquals(16384, SummaryPolicy.sized(4096).mergeBudgetBytes(dictionaryPolicy));
        assertEquals(2048, new SummaryPolicy(256, 2).mergeBudgetBytes(dictionaryPolicy));
    }
}
