/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.OpenJobAction.Request;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class OpenJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }
}