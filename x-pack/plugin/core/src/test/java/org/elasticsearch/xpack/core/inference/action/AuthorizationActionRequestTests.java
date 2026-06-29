/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class AuthorizationActionRequestTests extends AbstractBWCWireSerializationTestCase<AuthorizationAction.Request> {

    @Override
    protected AuthorizationAction.Request mutateInstanceForVersion(AuthorizationAction.Request instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AuthorizationAction.Request> instanceReader() {
        return AuthorizationAction.Request::new;
    }

    @Override
    protected AuthorizationAction.Request createTestInstance() {
        return new AuthorizationAction.Request();
    }

    @Override
    protected AuthorizationAction.Request mutateInstance(AuthorizationAction.Request instance) throws IOException {
        // need to return null here because the instances will always be identical
        return null;
    }
}
