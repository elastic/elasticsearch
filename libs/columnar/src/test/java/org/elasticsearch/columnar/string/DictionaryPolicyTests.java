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

/** The bounds a column's dictionary is chosen under, on their own. */
public class DictionaryPolicyTests extends ESTestCase {

    /** A dictionary is bounded both absolutely and against what it describes, and the tighter one governs. */
    public void testBudgetTakesTheTighterBound() {
        final DictionaryPolicy policy = new DictionaryPolicy(1000, 0.5, 0.2);
        // A small column: the share binds, so the dictionary may not approach the size of the values.
        assertEquals(200, policy.budgetFor(1000));
        // A large one: the absolute bound binds, so it stops growing with the column.
        assertEquals(1000, policy.budgetFor(1_000_000));
        // Exactly where they cross.
        assertEquals(1000, policy.budgetFor(5000));
    }

    /** A column with no values has no budget, rather than a negative or absolute one. */
    public void testEmptyColumnHasNoBudget() {
        assertEquals(0, new DictionaryPolicy(1000, 0.5, 0.2).budgetFor(0));
    }

    public void testNoneNeverBuildsADictionary() {
        assertFalse(DictionaryPolicy.NONE.enabled());
        assertEquals(0, DictionaryPolicy.NONE.budgetFor(1_000_000));
    }

    public void testEnabledFollowsTheByteBound() {
        assertTrue(new DictionaryPolicy(1, 0.5, 0.2).enabled());
        assertFalse(new DictionaryPolicy(0, 0.5, 0.2).enabled());
    }

    /** Both questions have to be answered: enough of the column named, and small against what it describes. */
    public void testWorthKeepingAsksBoth() {
        final DictionaryPolicy policy = new DictionaryPolicy(1_000_000, 0.5, 0.2);
        assertTrue("covers enough and is small", policy.worthKeeping(0.9, 100, 10_000));
        assertFalse("covers too little", policy.worthKeeping(0.4, 100, 10_000));
        assertFalse("too large against the column", policy.worthKeeping(0.9, 5_000, 10_000));
        assertTrue("exactly at both bounds", policy.worthKeeping(0.5, 2_000, 10_000));
    }

    public void testRejectsBoundsThatAreNotShares() {
        expectThrows(IllegalArgumentException.class, () -> new DictionaryPolicy(-1, 0.5, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new DictionaryPolicy(1000, -0.1, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new DictionaryPolicy(1000, 1.1, 0.2));
        expectThrows(IllegalArgumentException.class, () -> new DictionaryPolicy(1000, 0.5, -0.1));
    }
}
