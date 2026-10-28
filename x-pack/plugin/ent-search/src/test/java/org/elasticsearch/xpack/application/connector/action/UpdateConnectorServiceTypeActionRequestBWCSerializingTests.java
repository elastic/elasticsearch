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
        String originalConnectorId = instance.getConnectorId();
        String serviceType = instance.getServiceType();
        switch (randomIntBetween(0, 1)) {
            case 0 -> originalConnectorId = randomValueOtherThan(originalConnectorId, () -> randomUUID());
            case 1 -> serviceType = randomValueOtherThan(serviceType, () -> randomAlphaOfLengthBetween(3, 10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UpdateConnectorServiceTypeAction.Request(originalConnectorId, serviceType);
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
