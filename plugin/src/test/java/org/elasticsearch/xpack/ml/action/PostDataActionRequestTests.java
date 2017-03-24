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
        PostDataAction.Request request = new PostDataAction.Request(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setResetStart(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setResetEnd(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setDataDescription(new DataDescription(randomFrom(DataFormat.values()),
                    randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                    randomAsciiOfLength(1).charAt(0), randomAsciiOfLength(1).charAt(0)));
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
