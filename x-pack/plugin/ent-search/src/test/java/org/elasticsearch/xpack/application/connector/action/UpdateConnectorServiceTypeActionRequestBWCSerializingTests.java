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

public class UpdateConnectorServiceTypeActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorServiceTypeAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorServiceTypeAction.Request> instanceReader() {
        return UpdateConnectorServiceTypeAction.Request::new;
    }

    @Override
    protected UpdateConnectorServiceTypeAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorServiceTypeAction.Request(connectorId, randomAlphaOfLengthBetween(3, 10));
    }

    @Override
    protected UpdateConnectorServiceTypeAction.Request mutateInstance(UpdateConnectorServiceTypeAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorServiceTypeAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorServiceTypeAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorServiceTypeAction.Request mutateInstanceForVersion(
        UpdateConnectorServiceTypeAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
