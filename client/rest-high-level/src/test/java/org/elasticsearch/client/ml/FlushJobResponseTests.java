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
import java.util.Date;

public class FlushJobResponseTests extends AbstractXContentTestCase<FlushJobResponse> {

    @Override
    protected FlushJobResponse createTestInstance() {
        return new FlushJobResponse(randomBoolean(),
            randomBoolean() ? null : new Date(randomNonNegativeLong()));
    }

    @Override
    protected FlushJobResponse doParseInstance(XContentParser parser) throws IOException {
        return FlushJobResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
