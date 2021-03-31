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

public class DeleteServiceAccountTokenRequestTests extends AbstractWireSerializingTestCase<DeleteServiceAccountTokenRequest> {

    @Override
    protected Writeable.Reader<DeleteServiceAccountTokenRequest> instanceReader() {
        return DeleteServiceAccountTokenRequest::new;
    }

    @Override
    protected DeleteServiceAccountTokenRequest createTestInstance() {
        return new DeleteServiceAccountTokenRequest(
            randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
    }

    @Override
    protected DeleteServiceAccountTokenRequest mutateInstance(DeleteServiceAccountTokenRequest instance) throws IOException {
        DeleteServiceAccountTokenRequest newInstance = instance;
        if (randomBoolean()) {
            newInstance = new DeleteServiceAccountTokenRequest(
                randomValueOtherThan(newInstance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                newInstance.getServiceName(), newInstance.getTokenName());
        }
        if (randomBoolean()) {
            newInstance = new DeleteServiceAccountTokenRequest(
                newInstance.getNamespace(),
                randomValueOtherThan(newInstance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                newInstance.getTokenName());
        }
        if (newInstance == instance || randomBoolean()) {
            newInstance = new DeleteServiceAccountTokenRequest(
                newInstance.getNamespace(), newInstance.getServiceName(),
                randomValueOtherThan(newInstance.getTokenName(), () -> randomAlphaOfLengthBetween(3, 8)));
        }
        return newInstance;
    }
}
