/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class FlushJobRequestTests extends AbstractXContentTestCase<FlushJobRequest> {

    @Override
    protected FlushJobRequest createTestInstance() {
        FlushJobRequest request = new FlushJobRequest(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setCalcInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setAdvanceTime(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setStart(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setEnd(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setSkipTime(String.valueOf(randomLong()));
        }
        return request;
    }

    @Override
    protected FlushJobRequest doParseInstance(XContentParser parser) throws IOException {
        return FlushJobRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
