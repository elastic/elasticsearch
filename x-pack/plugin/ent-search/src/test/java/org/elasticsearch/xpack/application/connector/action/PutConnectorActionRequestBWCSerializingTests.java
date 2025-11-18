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
        String originalConnectorId = instance.getConnectorId();
        String description = instance.getDescription();
        String indexName = instance.getIndexName();
        Boolean isNative = instance.getIsNative();
        String language = instance.getLanguage();
        String name = instance.getName();
        String serviceType = instance.getServiceType();
        switch (between(0, 6)) {
            case 0 -> originalConnectorId = randomValueOtherThan(originalConnectorId, () -> randomAlphaOfLengthBetween(5, 15));
            case 1 -> description = randomValueOtherThan(description, () -> randomAlphaOfLengthBetween(5, 15));
            case 2 -> indexName = randomValueOtherThan(indexName, () -> randomAlphaOfLengthBetween(5, 15));
            case 3 -> isNative = isNative == false;
            case 4 -> language = randomValueOtherThan(language, () -> randomAlphaOfLengthBetween(5, 15));
            case 5 -> name = randomValueOtherThan(name, () -> randomAlphaOfLengthBetween(5, 15));
            case 6 -> serviceType = randomValueOtherThan(serviceType, () -> randomAlphaOfLengthBetween(5, 15));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutConnectorAction.Request(originalConnectorId, description, indexName, isNative, language, name, serviceType);
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
