/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.entsearch.engine.Engine;

public class PutEngineActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutEngineAction.Request> {

    private static Engine randomEngine() {
        return new Engine(
            ESTestCase.randomAlphaOfLengthBetween(1, 10),
            generateRandomStringArray(10, 10, false, false),
            randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) })
        );
    }

    @Override
    protected Writeable.Reader<PutEngineAction.Request> instanceReader() {
        return PutEngineAction.Request::new;
    }

    @Override
    protected PutEngineAction.Request createTestInstance() {
        return new PutEngineAction.Request(randomEngine());
    }

    @Override
    protected PutEngineAction.Request mutateInstance(PutEngineAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
