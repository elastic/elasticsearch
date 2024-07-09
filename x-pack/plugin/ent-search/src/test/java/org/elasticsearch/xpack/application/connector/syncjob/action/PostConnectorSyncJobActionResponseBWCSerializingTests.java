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

public class PostConnectorSyncJobActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    PostConnectorSyncJobAction.Response> {

    @Override
    protected Writeable.Reader<PostConnectorSyncJobAction.Response> instanceReader() {
        return PostConnectorSyncJobAction.Response::new;
    }

    @Override
    protected PostConnectorSyncJobAction.Response createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomPostConnectorSyncJobActionResponse();
    }

    @Override
    protected PostConnectorSyncJobAction.Response mutateInstance(PostConnectorSyncJobAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PostConnectorSyncJobAction.Response mutateInstanceForVersion(
        PostConnectorSyncJobAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }

}
