/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetSecretRequestTests extends AbstractWireSerializingTestCase<GetSecretRequest> {

    @Override
    protected Writeable.Reader<GetSecretRequest> instanceReader() {
        return GetSecretRequest::new;
    }

    @Override
    protected GetSecretRequest createTestInstance() {
        return new GetSecretRequest(randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected GetSecretRequest mutateInstance(GetSecretRequest instance) {
        return new GetSecretRequest(instance.id() + randomAlphaOfLength(1));
    }
}
