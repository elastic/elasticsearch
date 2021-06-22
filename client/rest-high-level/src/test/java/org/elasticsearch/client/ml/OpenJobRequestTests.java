/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class OpenJobRequestTests extends AbstractXContentTestCase<OpenJobRequest> {

    @Override
    protected OpenJobRequest createTestInstance() {
        OpenJobRequest openJobRequest = new OpenJobRequest(JobTests.randomValidJobId());
        if (randomBoolean()) {
            openJobRequest.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, Integer.MAX_VALUE)));
        }
        return openJobRequest;
    }

    @Override
    protected OpenJobRequest doParseInstance(XContentParser parser) throws IOException {
        return OpenJobRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
