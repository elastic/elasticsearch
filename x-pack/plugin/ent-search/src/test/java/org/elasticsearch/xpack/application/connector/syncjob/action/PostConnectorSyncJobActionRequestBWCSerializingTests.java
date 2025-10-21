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
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTriggerMethod;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;

import java.io.IOException;

public class PostConnectorSyncJobActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    PostConnectorSyncJobAction.Request> {

    @Override
    protected Writeable.Reader<PostConnectorSyncJobAction.Request> instanceReader() {
        return PostConnectorSyncJobAction.Request::new;
    }

    @Override
    protected PostConnectorSyncJobAction.Request createTestInstance() {
        return ConnectorSyncJobTestUtils.getRandomPostConnectorSyncJobActionRequest();
    }

    @Override
    protected PostConnectorSyncJobAction.Request mutateInstance(PostConnectorSyncJobAction.Request instance) throws IOException {
        String id = instance.getId();
        ConnectorSyncJobType jobType = instance.getJobType();
        ConnectorSyncJobTriggerMethod triggerMethod = instance.getTriggerMethod();
        switch (randomIntBetween(0, 2)) {
            case 0 -> id = randomValueOtherThan(id, () -> randomAlphaOfLengthBetween(5, 15));
            case 1 -> jobType = randomValueOtherThan(jobType, () -> randomFrom(ConnectorSyncJobType.values()));
            case 2 -> triggerMethod = randomValueOtherThan(triggerMethod, () -> randomFrom(ConnectorSyncJobTriggerMethod.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PostConnectorSyncJobAction.Request(id, jobType, triggerMethod);
    }

    @Override
    protected PostConnectorSyncJobAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PostConnectorSyncJobAction.Request.fromXContent(parser);
    }

    @Override
    protected PostConnectorSyncJobAction.Request mutateInstanceForVersion(
        PostConnectorSyncJobAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
