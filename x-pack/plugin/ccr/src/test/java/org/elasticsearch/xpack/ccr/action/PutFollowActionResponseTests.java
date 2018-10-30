/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

public class PutFollowActionResponseTests extends AbstractStreamableTestCase<PutFollowAction.Response> {

    @Override
    protected PutFollowAction.Response createBlankInstance() {
        return new PutFollowAction.Response();
    }

    @Override
    protected PutFollowAction.Response createTestInstance() {
        return new PutFollowAction.Response(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
