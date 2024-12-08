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
import java.util.List;

public class UpdateConnectorFilteringActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorFilteringAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorFilteringAction.Request> instanceReader() {
        return UpdateConnectorFilteringAction.Request::new;
    }

    @Override
    protected UpdateConnectorFilteringAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorFilteringAction.Request(
            connectorId,
            List.of(ConnectorTestUtils.getRandomConnectorFiltering(), ConnectorTestUtils.getRandomConnectorFiltering()),
            ConnectorTestUtils.getRandomConnectorFiltering().getActive().getAdvancedSnippet(),
            ConnectorTestUtils.getRandomConnectorFiltering().getActive().getRules()
        );
    }

    @Override
    protected UpdateConnectorFilteringAction.Request mutateInstance(UpdateConnectorFilteringAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorFilteringAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorFilteringAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorFilteringAction.Request mutateInstanceForVersion(
        UpdateConnectorFilteringAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
