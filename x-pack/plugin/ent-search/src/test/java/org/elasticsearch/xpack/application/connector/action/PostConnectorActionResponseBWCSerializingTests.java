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

public class PostConnectorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<ConnectorCreateActionResponse> {
    @Override
    protected Writeable.Reader<ConnectorCreateActionResponse> instanceReader() {
        return ConnectorCreateActionResponse::new;
    }

    @Override
    protected ConnectorCreateActionResponse createTestInstance() {
        return new ConnectorCreateActionResponse(randomUUID(), randomFrom(DocWriteResponse.Result.values()));
    }

    @Override
    protected ConnectorCreateActionResponse mutateInstance(ConnectorCreateActionResponse instance) throws IOException {
        String id = instance.getId();
        DocWriteResponse.Result result = instance.getResult();
        switch (randomIntBetween(0, 1)) {
            case 0 -> id = randomValueOtherThan(id, () -> randomUUID());
            case 1 -> result = randomValueOtherThan(result, () -> randomFrom(DocWriteResponse.Result.values()));
        }
        return new ConnectorCreateActionResponse(id, result);
    }

    @Override
    protected ConnectorCreateActionResponse mutateInstanceForVersion(ConnectorCreateActionResponse instance, TransportVersion version) {
        return instance;
    }
}
