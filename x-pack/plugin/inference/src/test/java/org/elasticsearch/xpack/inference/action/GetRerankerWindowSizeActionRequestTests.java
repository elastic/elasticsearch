/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;

import java.io.IOException;

public class GetRerankerWindowSizeActionRequestTests extends AbstractWireSerializingTestCase<GetRerankerWindowSizeAction.Request> {
    @Override
    protected Writeable.Reader<GetRerankerWindowSizeAction.Request> instanceReader() {
        return GetRerankerWindowSizeAction.Request::new;
    }

    @Override
    protected GetRerankerWindowSizeAction.Request createTestInstance() {
        return new GetRerankerWindowSizeAction.Request(randomAlphaOfLength(8));
    }

    @Override
    protected GetRerankerWindowSizeAction.Request mutateInstance(GetRerankerWindowSizeAction.Request instance) throws IOException {
        return new GetRerankerWindowSizeAction.Request(randomValueOtherThan(instance.getInferenceEntityId(), () -> randomAlphaOfLength(8)));
    }
}
