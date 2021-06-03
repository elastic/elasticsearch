/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class GetServiceAccountRequestTests extends AbstractWireSerializingTestCase<GetServiceAccountRequest> {

    @Override
    protected Writeable.Reader<GetServiceAccountRequest> instanceReader() {
        return GetServiceAccountRequest::new;
    }

    @Override
    protected GetServiceAccountRequest createTestInstance() {
        return new GetServiceAccountRequest(randomFrom(randomAlphaOfLengthBetween(3, 8), null),
            randomFrom(randomAlphaOfLengthBetween(3, 8), null));
    }

    @Override
    protected GetServiceAccountRequest mutateInstance(GetServiceAccountRequest instance) throws IOException {
        if (randomBoolean()) {
            return new GetServiceAccountRequest(
                randomValueOtherThan(instance.getNamespace(), () -> randomFrom(randomAlphaOfLengthBetween(3, 8), null)),
                instance.getServiceName());
        } else {
            return new GetServiceAccountRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), () -> randomFrom(randomAlphaOfLengthBetween(3, 8), null)));
        }
    }
}
