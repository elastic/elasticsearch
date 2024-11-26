/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class UpdateConnectorActiveFilteringActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    UpdateConnectorActiveFilteringAction.Request> {

    private String connectorId;

    @Override
    protected Writeable.Reader<UpdateConnectorActiveFilteringAction.Request> instanceReader() {
        return UpdateConnectorActiveFilteringAction.Request::new;
    }

    @Override
    protected UpdateConnectorActiveFilteringAction.Request createTestInstance() {
        this.connectorId = randomUUID();
        return new UpdateConnectorActiveFilteringAction.Request(connectorId);
    }

    @Override
    protected UpdateConnectorActiveFilteringAction.Request mutateInstance(UpdateConnectorActiveFilteringAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorActiveFilteringAction.Request doParseInstance(XContentParser parser) throws IOException {
        return new UpdateConnectorActiveFilteringAction.Request(this.connectorId);
    }

    @Override
    protected UpdateConnectorActiveFilteringAction.Request mutateInstanceForVersion(
        UpdateConnectorActiveFilteringAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
