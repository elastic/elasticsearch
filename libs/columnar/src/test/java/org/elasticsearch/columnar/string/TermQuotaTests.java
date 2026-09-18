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

public class TermQuotaTests extends ESTestCase {

    private static final DictionaryPolicy DICTIONARY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
    private static final SummaryPolicy VOCABULARY = new SummaryPolicy(512 * 1024);

    public void testADictionaryRefusesTermsHeldOnce() {
        assertEquals(2, TermQuota.forDictionary(DICTIONARY, 1_000_000).minCount());
    }

    public void testASummaryAdmitsEveryTermTheSurveySaw() {
        assertEquals(1, TermQuota.forSummary(VOCABULARY).minCount());
    }

    // NOTE: a dictionary answers to the column it names, so the share of that column bounds it below the cap.
    public void testADictionaryIsBoundedByTheShareOfItsColumn() {
        assertEquals(200_000, TermQuota.forDictionary(DICTIONARY, 1_000_000).budget());
        assertEquals(DICTIONARY.maxBytes(), TermQuota.forDictionary(DICTIONARY, Long.MAX_VALUE / 2).budget());
    }

    // NOTE: a summary answers to a merge reading it against a far larger column, where a share of this one means nothing.
    public void testASummaryIsBoundedByTheCapAlone() {
        assertEquals(VOCABULARY.maxBytes(), TermQuota.forSummary(VOCABULARY).budget());
        assertEquals(0, TermQuota.forSummary(SummaryPolicy.NONE).budget());
    }
}
