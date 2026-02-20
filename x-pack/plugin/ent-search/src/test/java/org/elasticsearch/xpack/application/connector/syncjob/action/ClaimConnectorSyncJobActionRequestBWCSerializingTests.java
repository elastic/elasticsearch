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
import java.util.Map;

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
        String jobId = instance.getConnectorSyncJobId();
        String workerHostname = instance.getWorkerHostname();
        Object syncCursor = instance.getSyncCursor();
        switch (randomIntBetween(0, 2)) {
            case 0 -> jobId = randomValueOtherThan(jobId, () -> randomAlphaOfLength(10));
            case 1 -> workerHostname = randomValueOtherThan(workerHostname, () -> randomAlphaOfLengthBetween(10, 100));
            case 2 -> syncCursor = randomValueOtherThan(syncCursor, () -> randomBoolean() ? Map.of("test", "123") : null);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ClaimConnectorSyncJobAction.Request(jobId, workerHostname, syncCursor);
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
