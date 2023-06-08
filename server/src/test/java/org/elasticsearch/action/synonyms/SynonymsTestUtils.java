/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.synonyms.SynonymRule;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;

class SynonymsTestUtils {

    private SynonymsTestUtils() {
        throw new UnsupportedOperationException();
    }

    static SynonymRule[] randomSynonymsSet() {
        return randomArray(10, SynonymRule[]::new, SynonymsTestUtils::randomSynonymRule);
    }

    static SynonymRule randomSynonymRule() {
        return new SynonymRule(
            randomBoolean() ? null : randomIdentifier(),
            String.join(", ", randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(1, 10)))
        );
    }
}
