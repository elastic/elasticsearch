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

public class UpdateConnectorNameActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorNameAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorNameAction.Request> instanceReader() {
        return UpdateConnectorNameAction.Request::new;
    }

    @Override
    protected UpdateConnectorNameAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorNameAction.Request(connectorId, randomAlphaOfLengthBetween(5, 15), randomAlphaOfLengthBetween(5, 15));
    }

    @Override
    protected UpdateConnectorNameAction.Request mutateInstance(UpdateConnectorNameAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorNameAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorNameAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorNameAction.Request mutateInstanceForVersion(
        UpdateConnectorNameAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
