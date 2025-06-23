/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class GetConnectorSyncJobActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    GetConnectorSyncJobAction.Response> {

    @Override
    protected Writeable.Reader<GetConnectorSyncJobAction.Response> instanceReader() {
        return GetConnectorSyncJobAction.Response::new;
    }

    @Override
    protected GetConnectorSyncJobAction.Response createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomGetConnectorSyncJobResponse();
    }

    @Override
    protected GetConnectorSyncJobAction.Response mutateInstance(GetConnectorSyncJobAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorSyncJobAction.Response mutateInstanceForVersion(
        GetConnectorSyncJobAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
