/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class UpdateConnectorLastSeenActionRequestBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    UpdateConnectorLastSeenAction.Request> {

    @Override
    protected Writeable.Reader<UpdateConnectorLastSeenAction.Request> instanceReader() {
        return UpdateConnectorLastSeenAction.Request::new;
    }

    @Override
    protected UpdateConnectorLastSeenAction.Request createTestInstance() {
        return new UpdateConnectorLastSeenAction.Request(randomUUID());
    }

    @Override
    protected UpdateConnectorLastSeenAction.Request mutateInstance(UpdateConnectorLastSeenAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorLastSeenAction.Request mutateInstanceForVersion(
        UpdateConnectorLastSeenAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
