/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteSecretRequestTests extends AbstractWireSerializingTestCase<DeleteSecretRequest> {

    @Override
    protected Writeable.Reader<DeleteSecretRequest> instanceReader() {
        return DeleteSecretRequest::new;
    }

    @Override
    protected DeleteSecretRequest createTestInstance() {
        return new DeleteSecretRequest(randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected DeleteSecretRequest mutateInstance(DeleteSecretRequest instance) {
        return new DeleteSecretRequest(instance.id() + randomAlphaOfLength(1));
    }
}
