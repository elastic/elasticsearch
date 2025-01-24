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
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.Collections;

public class ListConnectorSyncJobsActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    ListConnectorSyncJobsAction.Request> {
    @Override
    protected Writeable.Reader<ListConnectorSyncJobsAction.Request> instanceReader() {
        return ListConnectorSyncJobsAction.Request::new;
    }

    @Override
    protected ListConnectorSyncJobsAction.Request createTestInstance() {
        PageParams pageParams = EnterpriseSearchModuleTestUtils.randomPageParams();
        String connectorId = randomAlphaOfLength(10);
        ConnectorSyncStatus syncStatus = ConnectorTestUtils.getRandomSyncStatus();
        ConnectorSyncJobType syncJobType = ConnectorTestUtils.getRandomSyncJobType();

        return new ListConnectorSyncJobsAction.Request(pageParams, connectorId, syncStatus, Collections.singletonList(syncJobType));
    }

    @Override
    protected ListConnectorSyncJobsAction.Request mutateInstance(ListConnectorSyncJobsAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListConnectorSyncJobsAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ListConnectorSyncJobsAction.Request.parse(parser);
    }

    @Override
    protected ListConnectorSyncJobsAction.Request mutateInstanceForVersion(
        ListConnectorSyncJobsAction.Request instance,
        TransportVersion version
    ) {
        return new ListConnectorSyncJobsAction.Request(
            instance.getPageParams(),
            instance.getConnectorId(),
            instance.getConnectorSyncStatus(),
            instance.getConnectorSyncJobTypeList()
        );
    }
}
