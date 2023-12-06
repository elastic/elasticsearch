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

public class UpdateConnectorNameActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    UpdateConnectorNameAction.Response> {

    @Override
    protected Writeable.Reader<UpdateConnectorNameAction.Response> instanceReader() {
        return UpdateConnectorNameAction.Response::new;
    }

    @Override
    protected UpdateConnectorNameAction.Response createTestInstance() {
        return new UpdateConnectorNameAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected UpdateConnectorNameAction.Response mutateInstance(UpdateConnectorNameAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorNameAction.Response mutateInstanceForVersion(
        UpdateConnectorNameAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
