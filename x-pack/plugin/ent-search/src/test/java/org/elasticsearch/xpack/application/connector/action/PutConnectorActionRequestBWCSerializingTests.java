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

public class PutConnectorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<PutConnectorAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<PutConnectorAction.Request> instanceReader() {
        return PutConnectorAction.Request::new;
    }

    @Override
    protected PutConnectorAction.Request createTestInstance() {
        PutConnectorAction.Request testInstance = ConnectorTestUtils.getRandomPutConnectorActionRequest();
        this.connectorId = testInstance.getConnectorId();
        return testInstance;
    }

    @Override
    protected PutConnectorAction.Request mutateInstance(PutConnectorAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutConnectorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutConnectorAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected PutConnectorAction.Request mutateInstanceForVersion(PutConnectorAction.Request instance, TransportVersion version) {
        return instance;
    }
}
