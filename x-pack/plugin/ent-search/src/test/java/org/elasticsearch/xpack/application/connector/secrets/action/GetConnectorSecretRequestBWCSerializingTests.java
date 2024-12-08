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

public class GetConnectorSecretRequestBWCSerializingTests extends AbstractBWCWireSerializationTestCase<GetConnectorSecretRequest> {

    @Override
    protected Writeable.Reader<GetConnectorSecretRequest> instanceReader() {
        return GetConnectorSecretRequest::new;
    }

    @Override
    protected GetConnectorSecretRequest createTestInstance() {
        return new GetConnectorSecretRequest(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected GetConnectorSecretRequest mutateInstance(GetConnectorSecretRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorSecretRequest mutateInstanceForVersion(GetConnectorSecretRequest instance, TransportVersion version) {
        return new GetConnectorSecretRequest(instance.id());
    }
}
