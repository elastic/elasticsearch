/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class DeleteConnectorSecretRequestBWCSerializingTests extends AbstractBWCWireSerializationTestCase<DeleteConnectorSecretRequest> {

    @Override
    protected Writeable.Reader<DeleteConnectorSecretRequest> instanceReader() {
        return DeleteConnectorSecretRequest::new;
    }

    @Override
    protected DeleteConnectorSecretRequest createTestInstance() {
        return new DeleteConnectorSecretRequest(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DeleteConnectorSecretRequest mutateInstance(DeleteConnectorSecretRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteConnectorSecretRequest mutateInstanceForVersion(DeleteConnectorSecretRequest instance, TransportVersion version) {
        return new DeleteConnectorSecretRequest(instance.id());
    }
}
