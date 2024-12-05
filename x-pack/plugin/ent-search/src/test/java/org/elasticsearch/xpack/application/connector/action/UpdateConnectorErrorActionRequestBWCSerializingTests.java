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

public class UpdateConnectorErrorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorErrorAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorErrorAction.Request> instanceReader() {
        return UpdateConnectorErrorAction.Request::new;
    }

    @Override
    protected UpdateConnectorErrorAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorErrorAction.Request(connectorId, randomAlphaOfLengthBetween(5, 15));
    }

    @Override
    protected UpdateConnectorErrorAction.Request mutateInstance(UpdateConnectorErrorAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorErrorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorErrorAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorErrorAction.Request mutateInstanceForVersion(
        UpdateConnectorErrorAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
