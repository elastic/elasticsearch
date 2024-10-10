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
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import java.io.IOException;

public class UpdateConnectorStatusActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorStatusAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorStatusAction.Request> instanceReader() {
        return UpdateConnectorStatusAction.Request::new;
    }

    @Override
    protected UpdateConnectorStatusAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorStatusAction.Request(connectorId, ConnectorTestUtils.getRandomConnectorStatus());
    }

    @Override
    protected UpdateConnectorStatusAction.Request mutateInstance(UpdateConnectorStatusAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorStatusAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorStatusAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorStatusAction.Request mutateInstanceForVersion(
        UpdateConnectorStatusAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
