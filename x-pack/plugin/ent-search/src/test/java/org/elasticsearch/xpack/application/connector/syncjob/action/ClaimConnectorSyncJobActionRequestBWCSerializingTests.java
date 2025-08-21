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

public class ClaimConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    ClaimConnectorSyncJobAction.Request> {

    public String connectorSyncJobId;

    @Override
    protected Writeable.Reader<ClaimConnectorSyncJobAction.Request> instanceReader() {
        return ClaimConnectorSyncJobAction.Request::new;
    }

    @Override
    protected ClaimConnectorSyncJobAction.Request createTestInstance() {
        ClaimConnectorSyncJobAction.Request request = ConnectorSyncJobTestUtils.getRandomClaimConnectorSyncJobActionRequest();
        connectorSyncJobId = request.getConnectorSyncJobId();
        return request;
    }

    @Override
    protected ClaimConnectorSyncJobAction.Request mutateInstance(ClaimConnectorSyncJobAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    protected ClaimConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ClaimConnectorSyncJobAction.Request.fromXContent(parser, connectorSyncJobId);
    }

    @Override
    protected ClaimConnectorSyncJobAction.Request mutateInstanceForVersion(
        ClaimConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return new ClaimConnectorSyncJobAction.Request(
            instance.getConnectorSyncJobId(),
            instance.getWorkerHostname(),
            instance.getSyncCursor()
        );
    }

}
