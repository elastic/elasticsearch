/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class DeleteConnectorSecretResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<DeleteConnectorSecretResponse> {

    @Override
    protected Writeable.Reader<DeleteConnectorSecretResponse> instanceReader() {
        return DeleteConnectorSecretResponse::new;
    }

    @Override
    protected DeleteConnectorSecretResponse createTestInstance() {
        return ConnectorSecretsTestUtils.getRandomDeleteConnectorSecretResponse();
    }

    @Override
    protected DeleteConnectorSecretResponse mutateInstance(DeleteConnectorSecretResponse instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected DeleteConnectorSecretResponse mutateInstanceForVersion(DeleteConnectorSecretResponse instance, TransportVersion version) {
        return instance;
    }
}
