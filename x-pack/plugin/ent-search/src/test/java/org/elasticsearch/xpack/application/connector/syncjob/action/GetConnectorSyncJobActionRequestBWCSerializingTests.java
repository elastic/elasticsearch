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

public class GetConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    GetConnectorSyncJobAction.Request> {
    @Override
    protected Writeable.Reader<GetConnectorSyncJobAction.Request> instanceReader() {
        return GetConnectorSyncJobAction.Request::new;
    }

    @Override
    protected GetConnectorSyncJobAction.Request createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomGetConnectorSyncJobRequest();
    }

    @Override
    protected GetConnectorSyncJobAction.Request mutateInstance(GetConnectorSyncJobAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return GetConnectorSyncJobAction.Request.parse(parser);
    }

    @Override
    protected GetConnectorSyncJobAction.Request mutateInstanceForVersion(
        GetConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return new GetConnectorSyncJobAction.Request(instance.getConnectorSyncJobId());
    }
}
