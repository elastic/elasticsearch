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

public class UpdateConnectorLastSeenActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    UpdateConnectorLastSeenAction.Response> {

    @Override
    protected Writeable.Reader<UpdateConnectorLastSeenAction.Response> instanceReader() {
        return UpdateConnectorLastSeenAction.Response::new;
    }

    @Override
    protected UpdateConnectorLastSeenAction.Response createTestInstance() {
        return new UpdateConnectorLastSeenAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected UpdateConnectorLastSeenAction.Response mutateInstance(UpdateConnectorLastSeenAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorLastSeenAction.Response mutateInstanceForVersion(
        UpdateConnectorLastSeenAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
