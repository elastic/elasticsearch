/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class PostConnectorActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<PostConnectorAction.Response> {
    @Override
    protected Writeable.Reader<PostConnectorAction.Response> instanceReader() {
        return PostConnectorAction.Response::new;
    }

    @Override
    protected PostConnectorAction.Response createTestInstance() {
        return new PostConnectorAction.Response(randomUUID());
    }

    @Override
    protected PostConnectorAction.Response mutateInstance(PostConnectorAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PostConnectorAction.Response mutateInstanceForVersion(PostConnectorAction.Response instance, TransportVersion version) {
        return instance;
    }
}
