/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetEngineActionRequestSerializingTests extends AbstractWireSerializingTestCase<GetEngineAction.Request> {

    @Override
    protected Writeable.Reader<GetEngineAction.Request> instanceReader() {
        return GetEngineAction.Request::new;
    }

    @Override
    protected GetEngineAction.Request createTestInstance() {
        return new GetEngineAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetEngineAction.Request mutateInstance(GetEngineAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
