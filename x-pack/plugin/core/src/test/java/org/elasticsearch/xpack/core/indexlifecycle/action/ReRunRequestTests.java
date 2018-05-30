/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.action.RetryAction.Request;

public class ReRunRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        String[] indices = new String[randomIntBetween(1, 10)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLengthBetween(2, 5);
        }
        return new Request(indices);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
