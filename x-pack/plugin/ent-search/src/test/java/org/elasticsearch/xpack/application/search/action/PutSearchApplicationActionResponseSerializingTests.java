/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PutSearchApplicationActionResponseSerializingTests extends AbstractWireSerializingTestCase<
    PutSearchApplicationAction.Response> {

    @Override
    protected Writeable.Reader<PutSearchApplicationAction.Response> instanceReader() {
        return PutSearchApplicationAction.Response::new;
    }

    @Override
    protected PutSearchApplicationAction.Response createTestInstance() {
        return new PutSearchApplicationAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected PutSearchApplicationAction.Response mutateInstance(PutSearchApplicationAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
