/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DataDescription.DataFormat;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

public class PostDataActionRequestTests extends AbstractStreamableTestCase<PostDataAction.Request> {
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
            request.setDataDescription(new DataDescription(randomFrom(DataFormat.values()),
                    randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20),
                    randomAlphaOfLength(1).charAt(0), randomAlphaOfLength(1).charAt(0)));
        }
        if (randomBoolean()) {
            request.setContent(new BytesArray(new byte[0]), randomFrom(XContentType.values()));
        }
        return request;
    }

    @Override
    protected PostDataAction.Request createBlankInstance() {
        return new PostDataAction.Request();
    }
}
