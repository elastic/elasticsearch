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

public class ListConnectorSyncJobsActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    ListConnectorSyncJobsAction.Response> {

    @Override
    protected Writeable.Reader<ListConnectorSyncJobsAction.Response> instanceReader() {
        return ListConnectorSyncJobsAction.Response::new;
    }

    @Override
    protected ListConnectorSyncJobsAction.Response createTestInstance() {
        return new ListConnectorSyncJobsAction.Response(
            randomList(10, ConnectorSyncJobTestUtils::getRandomSyncJobSearchResult),
            randomLongBetween(0, 100)
        );
    }

    @Override
    protected ListConnectorSyncJobsAction.Response mutateInstance(ListConnectorSyncJobsAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListConnectorSyncJobsAction.Response mutateInstanceForVersion(
        ListConnectorSyncJobsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
