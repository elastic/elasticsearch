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

public class GetConnectorSecretResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<GetConnectorSecretResponse> {

    @Override
    protected Writeable.Reader<GetConnectorSecretResponse> instanceReader() {
        return GetConnectorSecretResponse::new;
    }

    @Override
    protected GetConnectorSecretResponse createTestInstance() {
        return ConnectorSecretsTestUtils.getRandomGetConnectorSecretResponse();
    }

    @Override
    protected GetConnectorSecretResponse mutateInstance(GetConnectorSecretResponse instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorSecretResponse mutateInstanceForVersion(GetConnectorSecretResponse instance, TransportVersion version) {
        return instance;
    }
}
