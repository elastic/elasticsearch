/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class UpdateConnectorNativeActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorNativeAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorNativeAction.Request> instanceReader() {
        return UpdateConnectorNativeAction.Request::new;
    }

    @Override
    protected UpdateConnectorNativeAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorNativeAction.Request(connectorId, randomBoolean());
    }

    @Override
    protected UpdateConnectorNativeAction.Request mutateInstance(UpdateConnectorNativeAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorNativeAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorNativeAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorNativeAction.Request mutateInstanceForVersion(
        UpdateConnectorNativeAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
