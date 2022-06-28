/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ResetJobResponseTests extends AbstractXContentTestCase<ResetJobResponse> {

    @Override
    protected ResetJobResponse createTestInstance() {
        if (randomBoolean()) {
            return new ResetJobResponse(randomBoolean(), null);
        }
        return new ResetJobResponse(null, new TaskId(randomAlphaOfLength(20) + ":" + randomIntBetween(1, 100)));
    }

    @Override
    protected ResetJobResponse doParseInstance(XContentParser parser) throws IOException {
        return ResetJobResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
