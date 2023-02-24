/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteEngineActionRequestSerializingTests extends AbstractWireSerializingTestCase<DeleteEngineAction.Request> {

    @Override
    protected Writeable.Reader<DeleteEngineAction.Request> instanceReader() {
        return DeleteEngineAction.Request::new;
    }

    @Override
    protected DeleteEngineAction.Request createTestInstance() {
        return new DeleteEngineAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DeleteEngineAction.Request mutateInstance(DeleteEngineAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
