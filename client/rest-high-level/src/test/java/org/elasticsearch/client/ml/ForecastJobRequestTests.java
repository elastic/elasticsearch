/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.unit.TimeValue;
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
