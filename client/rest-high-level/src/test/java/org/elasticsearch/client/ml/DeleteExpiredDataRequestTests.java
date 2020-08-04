/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;


public class DeleteExpiredDataRequestTests extends AbstractXContentTestCase<DeleteExpiredDataRequest> {

    private static ConstructingObjectParser<DeleteExpiredDataRequest, Void> PARSER = new ConstructingObjectParser<>(
        "delete_expired_data_request",
        true,
        (a) -> new DeleteExpiredDataRequest((String) a[0], (Float) a[1], (TimeValue) a[2])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(),
            new ParseField(DeleteExpiredDataRequest.JOB_ID));
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(),
            new ParseField(DeleteExpiredDataRequest.REQUESTS_PER_SECOND));
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), DeleteExpiredDataRequest.TIMEOUT),
            new ParseField(DeleteExpiredDataRequest.TIMEOUT),
            ObjectParser.ValueType.STRING);
    }

    @Override
    protected DeleteExpiredDataRequest createTestInstance() {
        return new DeleteExpiredDataRequest(
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), "test"));
    }

    @Override
    protected DeleteExpiredDataRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
