/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.HashMap;
import java.util.Map;

public class OpenJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        OpenJobAction.JobParams params = new OpenJobAction.JobParams(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            params.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        return new Request(params);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }
}