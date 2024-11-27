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

public class UpdateConnectorIndexNameActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorIndexNameAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorIndexNameAction.Request> instanceReader() {
        return UpdateConnectorIndexNameAction.Request::new;
    }

    @Override
    protected UpdateConnectorIndexNameAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorIndexNameAction.Request(connectorId, randomAlphaOfLengthBetween(3, 10));
    }

    @Override
    protected UpdateConnectorIndexNameAction.Request mutateInstance(UpdateConnectorIndexNameAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorIndexNameAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorIndexNameAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorIndexNameAction.Request mutateInstanceForVersion(
        UpdateConnectorIndexNameAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
