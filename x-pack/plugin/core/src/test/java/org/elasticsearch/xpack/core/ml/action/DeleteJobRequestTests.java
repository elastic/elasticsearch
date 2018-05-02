/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;

public class DeleteJobRequestTests extends AbstractStreamableTestCase<DeleteJobAction.Request> {

    @Override
    protected DeleteJobAction.Request createTestInstance() {
        DeleteJobAction.Request request = new DeleteJobAction.Request(randomAlphaOfLengthBetween(1, 20));
        request.setForce(randomBoolean());
        return request;
    }

    @Override
    protected DeleteJobAction.Request createBlankInstance() {
        return new DeleteJobAction.Request();
    }
}