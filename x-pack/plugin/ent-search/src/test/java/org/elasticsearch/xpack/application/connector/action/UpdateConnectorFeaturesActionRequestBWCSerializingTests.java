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
import org.elasticsearch.xpack.application.connector.ConnectorFeatures;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import java.io.IOException;

public class UpdateConnectorFeaturesActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorFeaturesAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorFeaturesAction.Request> instanceReader() {
        return UpdateConnectorFeaturesAction.Request::new;
    }

    @Override
    protected UpdateConnectorFeaturesAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorFeaturesAction.Request(connectorId, ConnectorTestUtils.getRandomConnectorFeatures());
    }

    @Override
    protected UpdateConnectorFeaturesAction.Request mutateInstance(UpdateConnectorFeaturesAction.Request instance) throws IOException {
        String originalConnectorId = instance.getConnectorId();
        ConnectorFeatures connectorFeatures = instance.getFeatures();
        switch (randomIntBetween(0, 1)) {
            case 0 -> originalConnectorId = randomValueOtherThan(originalConnectorId, () -> randomUUID());
            case 1 -> connectorFeatures = randomValueOtherThan(connectorFeatures, ConnectorTestUtils::getRandomConnectorFeatures);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UpdateConnectorFeaturesAction.Request(originalConnectorId, connectorFeatures);
    }

    @Override
    protected UpdateConnectorFeaturesAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorFeaturesAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorFeaturesAction.Request mutateInstanceForVersion(
        UpdateConnectorFeaturesAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
