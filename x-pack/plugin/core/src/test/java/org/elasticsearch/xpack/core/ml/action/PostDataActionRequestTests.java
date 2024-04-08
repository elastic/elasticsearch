/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;

public class PostDataActionRequestTests extends AbstractWireSerializingTestCase<PostDataAction.Request> {
    @Override
    protected PostDataAction.Request createTestInstance() {
        PostDataAction.Request request = new PostDataAction.Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setResetStart(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setResetEnd(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setDataDescription(new DataDescription(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)));
        }
        if (randomBoolean()) {
            request.setContent(new BytesArray(new byte[0]), randomFrom(XContentType.values()));
        }
        return request;
    }

    @Override
    protected PostDataAction.Request mutateInstance(PostDataAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<PostDataAction.Request> instanceReader() {
        return PostDataAction.Request::new;
    }
}
