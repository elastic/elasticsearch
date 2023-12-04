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

public class UpdateConnectorErrorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    UpdateConnectorErrorAction.Response> {

    @Override
    protected Writeable.Reader<UpdateConnectorErrorAction.Response> instanceReader() {
        return UpdateConnectorErrorAction.Response::new;
    }

    @Override
    protected UpdateConnectorErrorAction.Response createTestInstance() {
        return new UpdateConnectorErrorAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected UpdateConnectorErrorAction.Response mutateInstance(UpdateConnectorErrorAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorErrorAction.Response mutateInstanceForVersion(
        UpdateConnectorErrorAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
