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

public class ConnectorUpdateActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<ConnectorUpdateActionResponse> {

    @Override
    protected Writeable.Reader<ConnectorUpdateActionResponse> instanceReader() {
        return ConnectorUpdateActionResponse::new;
    }

    @Override
    protected ConnectorUpdateActionResponse createTestInstance() {
        return new ConnectorUpdateActionResponse(randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected ConnectorUpdateActionResponse mutateInstance(ConnectorUpdateActionResponse instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ConnectorUpdateActionResponse mutateInstanceForVersion(ConnectorUpdateActionResponse instance, TransportVersion version) {
        return instance;
    }
}
