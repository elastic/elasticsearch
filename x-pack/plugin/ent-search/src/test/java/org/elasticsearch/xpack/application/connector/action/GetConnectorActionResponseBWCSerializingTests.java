/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.connector.ConnectorSearchResult;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class GetConnectorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<GetConnectorAction.Response> {

    private ConnectorSearchResult connector;

    @Override
    protected Writeable.Reader<GetConnectorAction.Response> instanceReader() {
        return GetConnectorAction.Response::new;
    }

    @Override
    protected GetConnectorAction.Response createTestInstance() {
        this.connector = ConnectorTestUtils.getRandomConnectorSearchResult();
        return new GetConnectorAction.Response(this.connector);
    }

    @Override
    protected GetConnectorAction.Response mutateInstance(GetConnectorAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetConnectorAction.Response mutateInstanceForVersion(GetConnectorAction.Response instance, TransportVersion version) {
        return instance;
    }
}
