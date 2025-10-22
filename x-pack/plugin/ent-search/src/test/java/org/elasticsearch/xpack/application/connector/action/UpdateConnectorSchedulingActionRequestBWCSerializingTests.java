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
import org.elasticsearch.xpack.application.connector.ConnectorScheduling;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import java.io.IOException;

public class UpdateConnectorSchedulingActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorSchedulingAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorSchedulingAction.Request> instanceReader() {
        return UpdateConnectorSchedulingAction.Request::new;
    }

    @Override
    protected UpdateConnectorSchedulingAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorSchedulingAction.Request(connectorId, ConnectorTestUtils.getRandomConnectorScheduling());
    }

    @Override
    protected UpdateConnectorSchedulingAction.Request mutateInstance(UpdateConnectorSchedulingAction.Request instance) throws IOException {
        String originalConnectorId = instance.getConnectorId();
        ConnectorScheduling connectorScheduling = instance.getScheduling();
        switch (randomIntBetween(0, 1)) {
            case 0 -> originalConnectorId = randomValueOtherThan(originalConnectorId, () -> randomUUID());
            case 1 -> connectorScheduling = randomValueOtherThan(connectorScheduling, ConnectorTestUtils::getRandomConnectorScheduling);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UpdateConnectorSchedulingAction.Request(originalConnectorId, connectorScheduling);
    }

    @Override
    protected UpdateConnectorSchedulingAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorSchedulingAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorSchedulingAction.Request mutateInstanceForVersion(
        UpdateConnectorSchedulingAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
