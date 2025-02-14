/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;

import java.io.IOException;

public class DeleteConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    DeleteConnectorSyncJobAction.Request> {

    @Override
    protected Writeable.Reader<DeleteConnectorSyncJobAction.Request> instanceReader() {
        return DeleteConnectorSyncJobAction.Request::new;
    }

    @Override
    protected DeleteConnectorSyncJobAction.Request createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomDeleteConnectorSyncJobActionRequest();
    }

    @Override
    protected DeleteConnectorSyncJobAction.Request mutateInstance(DeleteConnectorSyncJobAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return DeleteConnectorSyncJobAction.Request.parse(parser);
    }

    @Override
    protected DeleteConnectorSyncJobAction.Request mutateInstanceForVersion(
        DeleteConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return new DeleteConnectorSyncJobAction.Request(instance.getConnectorSyncJobId());
    }
}
