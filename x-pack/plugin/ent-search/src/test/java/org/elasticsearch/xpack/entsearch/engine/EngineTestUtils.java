/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;

import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public final class EngineTestUtils {

    private EngineTestUtils() {
        throw new UnsupportedOperationException("Don't instantiate this class!");
    }

    public static PageParams randomPageParams() {
        int from = randomIntBetween(0, 10000);
        int size = randomIntBetween(0, 10000);
        PageParams pageParams = new PageParams(from, size);
        return pageParams;
    }

    public static Engine randomEngine() {
        return new Engine(
            ESTestCase.randomAlphaOfLengthBetween(1, 10),
            generateRandomStringArray(10, 10, false, false),
            randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) })
        );
    }
}
