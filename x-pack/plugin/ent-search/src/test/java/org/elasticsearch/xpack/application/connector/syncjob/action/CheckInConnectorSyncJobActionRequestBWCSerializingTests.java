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

public class CheckInConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    CheckInConnectorSyncJobAction.Request> {
    @Override
    protected Writeable.Reader<CheckInConnectorSyncJobAction.Request> instanceReader() {
        return CheckInConnectorSyncJobAction.Request::new;
    }

    @Override
    protected CheckInConnectorSyncJobAction.Request createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomCheckInConnectorSyncJobActionRequest();
    }

    @Override
    protected CheckInConnectorSyncJobAction.Request mutateInstance(CheckInConnectorSyncJobAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected CheckInConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return CheckInConnectorSyncJobAction.Request.parse(parser);
    }

    @Override
    protected CheckInConnectorSyncJobAction.Request mutateInstanceForVersion(
        CheckInConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return new CheckInConnectorSyncJobAction.Request(instance.getConnectorSyncJobId());
    }
}
