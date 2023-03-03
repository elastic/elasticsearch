/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.entsearch.engine.EngineTestUtils;

public class PutEngineActionRequestSerializingTests extends AbstractWireSerializingTestCase<PutEngineAction.Request> {

    @Override
    protected Writeable.Reader<PutEngineAction.Request> instanceReader() {
        return PutEngineAction.Request::new;
    }

    @Override
    protected PutEngineAction.Request createTestInstance() {
        return new PutEngineAction.Request(EngineTestUtils.randomEngine(), randomBoolean());
    }

    @Override
    protected PutEngineAction.Request mutateInstance(PutEngineAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
