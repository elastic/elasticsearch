/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class AuthorizationTaskExecutorMessageTests extends AbstractBWCWireSerializationTestCase<AuthorizationTaskExecutor.Message> {

    @Override
    protected AuthorizationTaskExecutor.Message mutateInstanceForVersion(
        AuthorizationTaskExecutor.Message instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AuthorizationTaskExecutor.Message> instanceReader() {
        return AuthorizationTaskExecutor.Message::new;
    }

    @Override
    protected AuthorizationTaskExecutor.Message createTestInstance() {
        return new AuthorizationTaskExecutor.Message(randomBoolean());
    }

    @Override
    protected AuthorizationTaskExecutor.Message mutateInstance(AuthorizationTaskExecutor.Message instance) throws IOException {
        if (instance.enable()) {
            return AuthorizationTaskExecutor.Message.DISABLE_MESSAGE;
        } else {
            return AuthorizationTaskExecutor.Message.ENABLE_MESSAGE;
        }
    }
}
