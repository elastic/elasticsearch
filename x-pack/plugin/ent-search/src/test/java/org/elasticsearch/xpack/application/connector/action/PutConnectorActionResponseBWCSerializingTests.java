/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class PutConnectorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<PutConnectorAction.Response> {
    @Override
    protected Writeable.Reader<PutConnectorAction.Response> instanceReader() {
        return PutConnectorAction.Response::new;
    }

    @Override
    protected PutConnectorAction.Response createTestInstance() {
        return new PutConnectorAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected PutConnectorAction.Response mutateInstance(PutConnectorAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutConnectorAction.Response mutateInstanceForVersion(PutConnectorAction.Response instance, TransportVersion version) {
        return instance;
    }
}
