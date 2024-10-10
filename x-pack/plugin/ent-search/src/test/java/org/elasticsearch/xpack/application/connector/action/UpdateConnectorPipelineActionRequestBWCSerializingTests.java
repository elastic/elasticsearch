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

public class UpdateConnectorPipelineActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorPipelineAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorPipelineAction.Request> instanceReader() {
        return UpdateConnectorPipelineAction.Request::new;
    }

    @Override
    protected UpdateConnectorPipelineAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorPipelineAction.Request(connectorId, ConnectorTestUtils.getRandomConnectorIngestPipeline());
    }

    @Override
    protected UpdateConnectorPipelineAction.Request mutateInstance(UpdateConnectorPipelineAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorPipelineAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorPipelineAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorPipelineAction.Request mutateInstanceForVersion(
        UpdateConnectorPipelineAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
