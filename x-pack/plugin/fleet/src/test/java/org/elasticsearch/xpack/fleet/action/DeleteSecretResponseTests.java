/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeleteSecretResponseTests extends AbstractWireSerializingTestCase<DeleteSecretResponse> {

    @Override
    protected Writeable.Reader<DeleteSecretResponse> instanceReader() {
        return DeleteSecretResponse::new;
    }

    @Override
    protected DeleteSecretResponse createTestInstance() {
        return new DeleteSecretResponse(randomBoolean());
    }

    @Override
    protected DeleteSecretResponse mutateInstance(DeleteSecretResponse instance) {
        // return a response with the opposite boolean value
        return new DeleteSecretResponse(instance.isDeleted() == false);
    }
}
