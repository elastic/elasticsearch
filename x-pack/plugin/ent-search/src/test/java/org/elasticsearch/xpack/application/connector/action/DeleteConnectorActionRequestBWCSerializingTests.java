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

public class DeleteConnectorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<DeleteConnectorAction.Request> {

    @Override
    protected Writeable.Reader<DeleteConnectorAction.Request> instanceReader() {
        return DeleteConnectorAction.Request::new;
    }

    @Override
    protected DeleteConnectorAction.Request createTestInstance() {
        return new DeleteConnectorAction.Request(randomAlphaOfLengthBetween(1, 10), false);
    }

    @Override
    protected DeleteConnectorAction.Request mutateInstance(DeleteConnectorAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteConnectorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return DeleteConnectorAction.Request.parse(parser);
    }

    @Override
    protected DeleteConnectorAction.Request mutateInstanceForVersion(DeleteConnectorAction.Request instance, TransportVersion version) {
        return new DeleteConnectorAction.Request(instance.getConnectorId(), instance.shouldDeleteSyncJobs());
    }
}
