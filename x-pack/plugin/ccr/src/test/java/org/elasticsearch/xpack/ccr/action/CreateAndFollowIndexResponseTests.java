/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowAction;

public class CreateAndFollowIndexResponseTests extends AbstractStreamableTestCase<FollowAction.Response> {

    @Override
    protected FollowAction.Response createBlankInstance() {
        return new FollowAction.Response();
    }

    @Override
    protected FollowAction.Response createTestInstance() {
        return new FollowAction.Response(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
