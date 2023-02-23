/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PutEngineActionResponseSerializingTests extends AbstractWireSerializingTestCase<PutEngineAction.Response> {

    @Override
    protected Writeable.Reader<PutEngineAction.Response> instanceReader() {
        return PutEngineAction.Response::new;
    }

    @Override
    protected PutEngineAction.Response createTestInstance() {
        return new PutEngineAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected PutEngineAction.Response mutateInstance(PutEngineAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
