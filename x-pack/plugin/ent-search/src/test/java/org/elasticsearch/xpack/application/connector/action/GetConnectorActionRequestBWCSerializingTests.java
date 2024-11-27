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

public class GetConnectorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<GetConnectorAction.Request> {

    @Override
    protected Writeable.Reader<GetConnectorAction.Request> instanceReader() {
        return GetConnectorAction.Request::new;
    }

    @Override
    protected GetConnectorAction.Request createTestInstance() {
        return new GetConnectorAction.Request(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetConnectorAction.Request mutateInstance(GetConnectorAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetConnectorAction.Request.parse(parser);
    }

    @Override
    protected GetConnectorAction.Request mutateInstanceForVersion(GetConnectorAction.Request instance, TransportVersion version) {
        return new GetConnectorAction.Request(instance.getConnectorId());
    }
}
