/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction.Request;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReasonTests;

public class UpdateTrainedModelAllocationStateActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(10), randomAlphaOfLength(10), RoutingStateAndReasonTests.randomInstance());
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

}
