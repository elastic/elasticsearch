/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

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
        String jobId = instance.getConnectorSyncJobId();
        Long deletedDocumentCount = instance.getDeletedDocumentCount();
        Long indexedDocumentCount = instance.getIndexedDocumentCount();
        Long indexedDocumentVolume = instance.getIndexedDocumentVolume();
        Long totalDocumentCount = instance.getTotalDocumentCount();
        Instant lastSeen = instance.getLastSeen();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 6)) {
            case 0 -> jobId = randomValueOtherThan(jobId, () -> randomAlphaOfLength(10));
            case 1 -> deletedDocumentCount = randomValueOtherThan(deletedDocumentCount, () -> randomNonNegativeLong());
            case 2 -> indexedDocumentCount = randomValueOtherThan(indexedDocumentCount, () -> randomNonNegativeLong());
            case 3 -> indexedDocumentVolume = randomValueOtherThan(indexedDocumentVolume, () -> randomNonNegativeLong());
            case 4 -> totalDocumentCount = randomValueOtherThan(totalDocumentCount, () -> randomNonNegativeLong());
            case 5 -> lastSeen = randomValueOtherThan(lastSeen, () -> randomInstant());
            case 6 -> metadata = randomValueOtherThan(
                metadata,
                () -> randomMap(2, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)))
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UpdateConnectorSyncJobIngestionStatsAction.Request(
            jobId,
            deletedDocumentCount,
            indexedDocumentCount,
            indexedDocumentVolume,
            totalDocumentCount,
            lastSeen,
            metadata
        );
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
