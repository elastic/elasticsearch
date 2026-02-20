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

public class PostConnectorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<PostConnectorAction.Request> {

    @Override
    protected Writeable.Reader<PostConnectorAction.Request> instanceReader() {
        return PostConnectorAction.Request::new;
    }

    @Override
    protected PostConnectorAction.Request createTestInstance() {
        return ConnectorTestUtils.getRandomPostConnectorActionRequest();
    }

    @Override
    protected PostConnectorAction.Request mutateInstance(PostConnectorAction.Request instance) throws IOException {
        String description = instance.getDescription();
        String indexName = instance.getIndexName();
        Boolean isNative = instance.getIsNative();
        String language = instance.getLanguage();
        String name = instance.getName();
        String serviceType = instance.getServiceType();
        switch (between(0, 5)) {
            case 0 -> description = randomValueOtherThan(description, () -> randomAlphaOfLengthBetween(5, 15));
            case 1 -> indexName = randomValueOtherThan(indexName, () -> randomAlphaOfLengthBetween(5, 15));
            case 2 -> isNative = isNative == false;
            case 3 -> language = randomValueOtherThan(language, () -> randomAlphaOfLengthBetween(5, 15));
            case 4 -> name = randomValueOtherThan(name, () -> randomAlphaOfLengthBetween(5, 15));
            case 5 -> serviceType = randomValueOtherThan(serviceType, () -> randomAlphaOfLengthBetween(5, 15));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PostConnectorAction.Request(description, indexName, isNative, language, name, serviceType);
    }

    @Override
    protected PostConnectorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PostConnectorAction.Request.fromXContent(parser);
    }

    @Override
    protected PostConnectorAction.Request mutateInstanceForVersion(PostConnectorAction.Request instance, TransportVersion version) {
        return instance;
    }
}
