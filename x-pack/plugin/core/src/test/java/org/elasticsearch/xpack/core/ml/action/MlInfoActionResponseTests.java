/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction.Response;

import java.util.HashMap;
import java.util.Map;

public class MlInfoActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int size = randomInt(10);
        Map<String, Object> info = new HashMap<>();
        for (int j = 0; j < size; j++) {
            info.put(randomAlphaOfLength(20), randomAlphaOfLength(20));
        }
        return new Response(info);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}
