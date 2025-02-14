/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import java.io.IOException;

public class UpdateConnectorLastSyncStatsActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorLastSyncStatsAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorLastSyncStatsAction.Request> instanceReader() {
        return UpdateConnectorLastSyncStatsAction.Request::new;
    }

    @Override
    protected UpdateConnectorLastSyncStatsAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorLastSyncStatsAction.Request.Builder().setConnectorId(connectorId)
            .setSyncInfo(ConnectorTestUtils.getRandomConnectorSyncInfo())
            .setSyncCursor(randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))))
            .build();
    }

    @Override
    protected UpdateConnectorLastSyncStatsAction.Request mutateInstance(UpdateConnectorLastSyncStatsAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorLastSyncStatsAction.Request doParseInstance(XContentParser parser) throws IOException {
        return UpdateConnectorLastSyncStatsAction.Request.fromXContent(parser, this.connectorId);
    }

    @Override
    protected UpdateConnectorLastSyncStatsAction.Request mutateInstanceForVersion(
        UpdateConnectorLastSyncStatsAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
