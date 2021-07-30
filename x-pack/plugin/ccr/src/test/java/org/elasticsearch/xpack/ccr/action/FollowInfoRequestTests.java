/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;

public class FollowInfoRequestTests extends AbstractWireSerializingTestCase<FollowInfoAction.Request> {

    @Override
    protected Writeable.Reader<FollowInfoAction.Request> instanceReader() {
        return FollowInfoAction.Request::new;
    }

    @Override
    protected FollowInfoAction.Request createTestInstance() {
        FollowInfoAction.Request request = new FollowInfoAction.Request();
        request.setFollowerIndices(generateRandomStringArray(4, 4, true, false));
        return request;
    }
}
