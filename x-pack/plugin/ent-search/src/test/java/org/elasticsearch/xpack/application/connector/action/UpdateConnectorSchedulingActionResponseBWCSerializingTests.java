/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class UpdateConnectorSchedulingActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    UpdateConnectorSchedulingAction.Response> {

    @Override
    protected Writeable.Reader<UpdateConnectorSchedulingAction.Response> instanceReader() {
        return UpdateConnectorSchedulingAction.Response::new;
    }

    @Override
    protected UpdateConnectorSchedulingAction.Response createTestInstance() {
        return new UpdateConnectorSchedulingAction.Response(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected UpdateConnectorSchedulingAction.Response mutateInstance(UpdateConnectorSchedulingAction.Response instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected UpdateConnectorSchedulingAction.Response mutateInstanceForVersion(
        UpdateConnectorSchedulingAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
