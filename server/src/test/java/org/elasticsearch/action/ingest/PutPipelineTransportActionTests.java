/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.core.Tuple.tuple;

public class PutPipelineTransportActionTests extends ESTestCase {


    private static Tuple<String, Object> randomMapEntry() {
        return tuple(randomAlphaOfLength(5), randomObject());
    }

    private static Object randomObject() {
        return randomFrom(
            random(),
            ESTestCase::randomLong,
            () -> generateRandomStringArray(10, 5, true),
            () -> randomMap(3, 5, PutPipelineTransportActionTests::randomMapEntry),
            () -> randomAlphaOfLength(5),
            ESTestCase::randomTimeValue,
            ESTestCase::randomDouble
        );
    }
}
