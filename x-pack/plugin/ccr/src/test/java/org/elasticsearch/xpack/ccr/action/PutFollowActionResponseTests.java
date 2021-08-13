/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

public class PutFollowActionResponseTests extends AbstractWireSerializingTestCase<PutFollowAction.Response> {

    @Override
    protected Writeable.Reader<PutFollowAction.Response> instanceReader() {
        return PutFollowAction.Response::new;
    }

    @Override
    protected PutFollowAction.Response createTestInstance() {
        return new PutFollowAction.Response(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
