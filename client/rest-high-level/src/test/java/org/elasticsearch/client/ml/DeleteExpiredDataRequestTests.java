/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
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
