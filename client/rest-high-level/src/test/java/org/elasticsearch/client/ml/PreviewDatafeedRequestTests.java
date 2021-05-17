/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PreviewDatafeedRequestTests extends AbstractXContentTestCase<PreviewDatafeedRequest> {

    @Override
    protected PreviewDatafeedRequest createTestInstance() {
        return randomBoolean() ?
            new PreviewDatafeedRequest(DatafeedConfigTests.randomValidDatafeedId()) :
            new PreviewDatafeedRequest(DatafeedConfigTests.createRandom(), randomBoolean() ? null : JobTests.createRandomizedJob());
    }

    @Override
    protected PreviewDatafeedRequest doParseInstance(XContentParser parser) throws IOException {
        return PreviewDatafeedRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
