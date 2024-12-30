/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class ConnectorSerializingTests extends AbstractBWCWireSerializationTestCase<Connector> {

    @Override
    protected Writeable.Reader<Connector> instanceReader() {
        return Connector::new;
    }

    @Override
    protected Connector createTestInstance() {
        return ConnectorTestUtils.getRandomConnector();
    }

    @Override
    protected Connector mutateInstance(Connector instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Connector mutateInstanceForVersion(Connector instance, TransportVersion version) {
        return instance;
    }
}
