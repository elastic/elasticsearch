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

public class UpdateConnectorSyncJobErrorActionRequestBWCSerializationTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorSyncJobErrorAction.Request> {

    private String connectorSyncJobId;

    @Override
    protected Writeable.Reader<UpdateConnectorSyncJobErrorAction.Request> instanceReader() {
        return UpdateConnectorSyncJobErrorAction.Request::new;
    }

    @Override
    protected UpdateConnectorSyncJobErrorAction.Request createTestInstance() {
        UpdateConnectorSyncJobErrorAction.Request request = ConnectorSyncJobTestUtils.getRandomUpdateConnectorSyncJobErrorActionRequest();
        this.connectorSyncJobId = request.getConnectorSyncJobId();
        return request;
    }

    @Override
    protected UpdateConnectorSyncJobErrorAction.Request mutateInstance(UpdateConnectorSyncJobErrorAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorSyncJobErrorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorSyncJobErrorAction.Request.fromXContent(parser, this.connectorSyncJobId);
    }

    @Override
    protected UpdateConnectorSyncJobErrorAction.Request mutateInstanceForVersion(
        UpdateConnectorSyncJobErrorAction.Request instance,
        TransportVersion version
    ) {
        return new UpdateConnectorSyncJobErrorAction.Request(instance.getConnectorSyncJobId(), instance.getError());
    }
}
