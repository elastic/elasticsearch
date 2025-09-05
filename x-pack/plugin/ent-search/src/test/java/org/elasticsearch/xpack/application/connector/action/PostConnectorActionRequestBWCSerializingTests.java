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
        return randomValueOtherThan(instance, this::createTestInstance);
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
