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

public class CancelConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    CancelConnectorSyncJobAction.Request> {
    @Override
    protected Writeable.Reader<CancelConnectorSyncJobAction.Request> instanceReader() {
        return CancelConnectorSyncJobAction.Request::new;
    }

    @Override
    protected CancelConnectorSyncJobAction.Request createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomCancelConnectorSyncJobActionRequest();
    }

    @Override
    protected CancelConnectorSyncJobAction.Request mutateInstance(CancelConnectorSyncJobAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected CancelConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return CancelConnectorSyncJobAction.Request.parse(parser);
    }

    @Override
    protected CancelConnectorSyncJobAction.Request mutateInstanceForVersion(
        CancelConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return new CancelConnectorSyncJobAction.Request(instance.getConnectorSyncJobId());
    }
}
