/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class PostSecretResponseTests extends AbstractWireSerializingTestCase<PostSecretResponse> {

    @Override
    protected Writeable.Reader<PostSecretResponse> instanceReader() {
        return PostSecretResponse::new;
    }

    @Override
    protected PostSecretResponse createTestInstance() {
        return new PostSecretResponse(randomAlphaOfLengthBetween(2, 10));
    }

    @Override
    protected PostSecretResponse mutateInstance(PostSecretResponse instance) {
        String id = randomValueOtherThan(instance.id(), () -> randomAlphaOfLengthBetween(2, 10));
        return new PostSecretResponse(id);
    }
}
