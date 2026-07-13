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

public class AuthorizationTaskParamsTests extends AbstractBWCWireSerializationTestCase<AuthorizationTaskParams> {

    @Override
    protected AuthorizationTaskParams mutateInstanceForVersion(AuthorizationTaskParams instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AuthorizationTaskParams> instanceReader() {
        return AuthorizationTaskParams::new;
    }

    @Override
    protected AuthorizationTaskParams createTestInstance() {
        return new AuthorizationTaskParams();
    }

    @Override
    protected AuthorizationTaskParams mutateInstance(AuthorizationTaskParams instance) throws IOException {
        // need to return null here because the instances will always be identical
        return null;
    }
}
