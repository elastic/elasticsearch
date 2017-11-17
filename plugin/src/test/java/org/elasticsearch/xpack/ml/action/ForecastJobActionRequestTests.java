/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.ForecastJobAction.Request;

import java.time.Instant;

public class ForecastJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setEndTime(Instant.ofEpochMilli(randomNonNegativeLong()).toString());
        }
        if (randomBoolean()) {
            request.setDuration(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)).getStringRep());
        }

        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
