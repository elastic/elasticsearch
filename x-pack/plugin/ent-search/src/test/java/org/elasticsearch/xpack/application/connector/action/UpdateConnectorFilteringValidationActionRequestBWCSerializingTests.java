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

public class UpdateConnectorFilteringValidationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorFilteringValidationAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorFilteringValidationAction.Request> instanceReader() {
        return UpdateConnectorFilteringValidationAction.Request::new;
    }

    @Override
    protected UpdateConnectorFilteringValidationAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorFilteringValidationAction.Request(connectorId, ConnectorTestUtils.getRandomFilteringValidationInfo());
    }

    @Override
    protected UpdateConnectorFilteringValidationAction.Request mutateInstance(UpdateConnectorFilteringValidationAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorFilteringValidationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorFilteringValidationAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorFilteringValidationAction.Request mutateInstanceForVersion(
        UpdateConnectorFilteringValidationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
