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

public class UpdateConnectorApiKeyIdActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorApiKeyIdAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorApiKeyIdAction.Request> instanceReader() {
        return UpdateConnectorApiKeyIdAction.Request::new;
    }

    @Override
    protected UpdateConnectorApiKeyIdAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorApiKeyIdAction.Request(connectorId, randomAlphaOfLengthBetween(5, 15), randomAlphaOfLengthBetween(5, 15));
    }

    @Override
    protected UpdateConnectorApiKeyIdAction.Request mutateInstance(UpdateConnectorApiKeyIdAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorApiKeyIdAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorApiKeyIdAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorApiKeyIdAction.Request mutateInstanceForVersion(
        UpdateConnectorApiKeyIdAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
