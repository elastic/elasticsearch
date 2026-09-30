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
        assertTrue(new SummaryPolicy(1).enabled());
        assertFalse(new SummaryPolicy(0).enabled());
        assertFalse(SummaryPolicy.NONE.enabled());
    }

    public void testRejectsANegativeBound() {
        expectThrows(IllegalArgumentException.class, () -> new SummaryPolicy(-1));
    }

    // NOTE: a merged dictionary is held to the dictionary's cap, so a column leaving more behind than that would describe terms no
    // merge could name.
    public void testTheDefaultLeavesBehindWhatADictionaryCouldHold() {
        assertEquals(StringColumnOptions.DEFAULT_DICTIONARY.maxBytes(), StringColumnOptions.DEFAULT_SUMMARY.maxBytes());
    }
}
