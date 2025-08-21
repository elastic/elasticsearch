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

public class UpdateConnectorSyncJobIngestionStatsActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorSyncJobIngestionStatsAction.Request> {

    public String connectorSyncJobId;

    @Override
    protected Writeable.Reader<UpdateConnectorSyncJobIngestionStatsAction.Request> instanceReader() {
        return UpdateConnectorSyncJobIngestionStatsAction.Request::new;
    }

    @Override
    protected UpdateConnectorSyncJobIngestionStatsAction.Request createTestInstance() {
        UpdateConnectorSyncJobIngestionStatsAction.Request request = ConnectorSyncJobTestUtils
            .getRandomUpdateConnectorSyncJobIngestionStatsActionRequest();
        connectorSyncJobId = request.getConnectorSyncJobId();
        return request;
    }

    @Override
    protected UpdateConnectorSyncJobIngestionStatsAction.Request mutateInstance(UpdateConnectorSyncJobIngestionStatsAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorSyncJobIngestionStatsAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorSyncJobIngestionStatsAction.Request.fromXContent(parser, connectorSyncJobId);
    }

    @Override
    protected UpdateConnectorSyncJobIngestionStatsAction.Request mutateInstanceForVersion(
        UpdateConnectorSyncJobIngestionStatsAction.Request instance,
        TransportVersion version
    ) {
        return new UpdateConnectorSyncJobIngestionStatsAction.Request(
            instance.getConnectorSyncJobId(),
            instance.getDeletedDocumentCount(),
            instance.getIndexedDocumentCount(),
            instance.getIndexedDocumentVolume(),
            instance.getTotalDocumentCount(),
            instance.getLastSeen(),
            instance.getMetadata()
        );
    }
}
