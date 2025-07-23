/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.action.GetRerankerAction;

import java.io.IOException;

public class GetRerankerActionRequestTests extends AbstractWireSerializingTestCase<GetRerankerAction.Request> {
    @Override
    protected Writeable.Reader<GetRerankerAction.Request> instanceReader() {
        return GetRerankerAction.Request::new;
    }

    @Override
    protected GetRerankerAction.Request createTestInstance() {
        return new GetRerankerAction.Request(randomAlphaOfLength(8));
    }

    @Override
    protected GetRerankerAction.Request mutateInstance(GetRerankerAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
