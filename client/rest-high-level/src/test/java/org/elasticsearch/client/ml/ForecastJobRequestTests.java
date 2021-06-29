/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class ForecastJobRequestTests extends AbstractXContentTestCase<ForecastJobRequest> {

    @Override
    protected ForecastJobRequest createTestInstance() {
        ForecastJobRequest request = new ForecastJobRequest(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setExpiresIn(TimeValue.timeValueHours(randomInt(10)));
        }
        if (randomBoolean()) {
            request.setDuration(TimeValue.timeValueHours(randomIntBetween(24, 72)));
        }
        if (randomBoolean()) {
            request.setMaxModelMemory(new ByteSizeValue(randomLongBetween(
                new ByteSizeValue(1, ByteSizeUnit.MB).getBytes(),
                new ByteSizeValue(499, ByteSizeUnit.MB).getBytes())));
        }
        return request;
    }

    @Override
    protected ForecastJobRequest doParseInstance(XContentParser parser) throws IOException {
        return ForecastJobRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
