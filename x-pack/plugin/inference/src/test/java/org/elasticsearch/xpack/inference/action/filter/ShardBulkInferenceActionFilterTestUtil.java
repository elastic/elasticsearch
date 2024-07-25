/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;

public class ShardBulkInferenceActionFilterTestUtil {

    /**
     * Returns a randomly generated object for Semantic Text tests purpose.
     */
    public static Object randomInputCasesForSemanticText() {
        int randomInt = randomIntBetween(0, 4);
        return switch (randomInt) {
            case 0 -> randomAlphaOfLengthBetween(10, 20);
            case 1 -> randomInt();
            case 2 -> randomLong();
            case 3 -> randomFloat();
            case 4 -> randomBoolean();
            default -> randomDouble();
        };
    }
}
