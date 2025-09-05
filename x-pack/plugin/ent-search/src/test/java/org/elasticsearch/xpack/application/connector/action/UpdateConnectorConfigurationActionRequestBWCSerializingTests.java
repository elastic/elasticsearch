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

public class UpdateConnectorConfigurationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorConfigurationAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorConfigurationAction.Request> instanceReader() {
        return UpdateConnectorConfigurationAction.Request::new;
    }

    @Override
    protected UpdateConnectorConfigurationAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorConfigurationAction.Request(
            connectorId,
            ConnectorTestUtils.getRandomConnectorConfiguration(),
            ConnectorTestUtils.getRandomConnectorConfigurationValues()
        );
    }

    @Override
    protected UpdateConnectorConfigurationAction.Request mutateInstance(UpdateConnectorConfigurationAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorConfigurationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorConfigurationAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorConfigurationAction.Request mutateInstanceForVersion(
        UpdateConnectorConfigurationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
