/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class ListConnectorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<ListConnectorAction.Response> {
    @Override
    protected Writeable.Reader<ListConnectorAction.Response> instanceReader() {
        return ListConnectorAction.Response::new;
    }

    @Override
    protected ListConnectorAction.Response createTestInstance() {
        return new ListConnectorAction.Response(
            randomList(10, ConnectorTestUtils::getRandomConnectorSearchResult),
            randomLongBetween(0, 100)
        );
    }

    @Override
    protected ListConnectorAction.Response mutateInstance(ListConnectorAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListConnectorAction.Response mutateInstanceForVersion(ListConnectorAction.Response instance, TransportVersion version) {
        return instance;
    }
}
