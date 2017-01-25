/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction.Request;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class StartDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<StartDatafeedAction.Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLength(10), randomNonNegativeLong());
        if (randomBoolean()) {
            request.setEndTime(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.setStartTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
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
