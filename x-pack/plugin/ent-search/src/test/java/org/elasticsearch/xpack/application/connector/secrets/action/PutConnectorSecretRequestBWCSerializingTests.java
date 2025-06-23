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

public class PutConnectorSecretRequestBWCSerializingTests extends AbstractBWCWireSerializationTestCase<PutConnectorSecretRequest> {

    @Override
    protected Writeable.Reader<PutConnectorSecretRequest> instanceReader() {
        return PutConnectorSecretRequest::new;
    }

    @Override
    protected PutConnectorSecretRequest createTestInstance() {
        return ConnectorSecretsTestUtils.getRandomPutConnectorSecretRequest();
    }

    @Override
    protected PutConnectorSecretRequest mutateInstance(PutConnectorSecretRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutConnectorSecretRequest mutateInstanceForVersion(PutConnectorSecretRequest instance, TransportVersion version) {
        return instance;
    }
}
