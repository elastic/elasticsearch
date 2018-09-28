/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

public class PutFollowActionRequestTests extends AbstractStreamableTestCase<PutFollowAction.Request> {

    @Override
    protected PutFollowAction.Request createBlankInstance() {
        return new PutFollowAction.Request();
    }

    @Override
    protected PutFollowAction.Request createTestInstance() {
        return new PutFollowAction.Request(FollowIndexRequestTests.createTestRequest());
    }
}
