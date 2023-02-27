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

public class GetEngineActionResponseSerializingTests extends AbstractWireSerializingTestCase<GetEngineAction.Response> {

    @Override
    protected Writeable.Reader<GetEngineAction.Response> instanceReader() {
        return GetEngineAction.Response::new;
    }

    @Override
    protected GetEngineAction.Response createTestInstance() {
        return new GetEngineAction.Response(
            new Engine(
                ESTestCase.randomAlphaOfLengthBetween(1, 10),
                generateRandomStringArray(10, 10, false, false),
                randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) })
            )
        );
    }

    @Override
    protected GetEngineAction.Response mutateInstance(GetEngineAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
