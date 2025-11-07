/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;

import java.io.IOException;

public class GetRerankerWindowSizeActionResponseTests extends AbstractWireSerializingTestCase<GetRerankerWindowSizeAction.Response> {
    @Override
    protected Writeable.Reader<GetRerankerWindowSizeAction.Response> instanceReader() {
        return GetRerankerWindowSizeAction.Response::new;
    }

    @Override
    protected GetRerankerWindowSizeAction.Response createTestInstance() {
        return new GetRerankerWindowSizeAction.Response(randomNonNegativeInt());
    }

    @Override
    protected GetRerankerWindowSizeAction.Response mutateInstance(GetRerankerWindowSizeAction.Response instance) throws IOException {
        return new GetRerankerWindowSizeAction.Response(randomValueOtherThan(instance.getWindowSize(), ESTestCase::randomNonNegativeInt));
    }
}
