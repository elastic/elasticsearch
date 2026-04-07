/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymSetSummary;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

public class SynonymsTestUtils {

    private SynonymsTestUtils() {
        throw new UnsupportedOperationException();
    }

    public static SynonymRule[] randomSynonymsSet(int length) {
        return randomSynonymsSet(length, length);
    }

    public static SynonymRule[] randomSynonymsSet(int minLength, int maxLength) {
        return randomArray(minLength, maxLength, SynonymRule[]::new, SynonymsTestUtils::randomSynonymRule);
    }

    public static SynonymRule[] randomSynonymsSetWithoutIds(int minLength, int maxLength) {
        return randomArray(minLength, maxLength, SynonymRule[]::new, () -> randomSynonymRule(null));
    }

    static SynonymRule[] randomSynonymsSet() {
        return randomSynonymsSet(0, 10);
    }

    static SynonymSetSummary[] randomSynonymsSetSummary() {
        return randomArray(10, SynonymSetSummary[]::new, SynonymsTestUtils::randomSynonymSetSummary);
    }

    static SynonymRule randomSynonymRule() {
        return randomSynonymRule(randomBoolean() ? null : randomIdentifier());
    }

    public static SynonymRule randomSynonymRule(String id) {
        return new SynonymRule(id, String.join(", ", randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(1, 10))));
    }

    static SynonymSetSummary randomSynonymSetSummary() {
        return new SynonymSetSummary(randomLongBetween(1, 10000), randomIdentifier());
    }
}
