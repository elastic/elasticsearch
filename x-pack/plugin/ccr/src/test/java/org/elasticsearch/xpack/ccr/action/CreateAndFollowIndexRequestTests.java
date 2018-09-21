/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction;

public class CreateAndFollowIndexRequestTests extends AbstractStreamableTestCase<CreateAndFollowIndexAction.Request> {

    @Override
    protected CreateAndFollowIndexAction.Request createBlankInstance() {
        return new CreateAndFollowIndexAction.Request();
    }

    @Override
    protected CreateAndFollowIndexAction.Request createTestInstance() {
        return new CreateAndFollowIndexAction.Request(FollowIndexRequestTests.createTestRequest());
    }
}
