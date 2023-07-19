/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

public class PostSecretRequestTests extends AbstractWireSerializingTestCase<PostSecretRequest> {

    @Override
    protected Writeable.Reader<PostSecretRequest> instanceReader() {
        return PostSecretRequest::new;
    }

    @Override
    protected PostSecretRequest createTestInstance() {
        return new PostSecretRequest(randomAlphaOfLengthBetween(10, 100), randomFrom(XContentType.values()));
    }

    @Override
    protected PostSecretRequest mutateInstance(PostSecretRequest instance) {
        return new PostSecretRequest(instance.source() + randomAlphaOfLength(1), instance.xContentType());
    }
}
