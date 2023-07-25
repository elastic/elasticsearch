/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PostSecretResponseTests extends AbstractWireSerializingTestCase<PostSecretResponse> {

    @Override
    protected Writeable.Reader<PostSecretResponse> instanceReader() {
        return PostSecretResponse::new;
    }

    @Override
    protected PostSecretResponse createTestInstance() {
        return new PostSecretResponse(randomFrom(RestStatus.OK, RestStatus.CREATED), randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected PostSecretResponse mutateInstance(PostSecretResponse instance) {
        if (instance.status() == RestStatus.OK) {
            return new PostSecretResponse(RestStatus.CREATED, randomAlphaOfLengthBetween(2, 10));
        }
        return new PostSecretResponse(RestStatus.OK, randomAlphaOfLengthBetween(2, 10));
    }
}
