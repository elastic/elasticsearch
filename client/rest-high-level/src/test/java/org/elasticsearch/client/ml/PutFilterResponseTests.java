/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.MlFilterTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PutFilterResponseTests extends AbstractXContentTestCase<PutFilterResponse> {

    @Override
    protected PutFilterResponse createTestInstance() {
        return new PutFilterResponse(MlFilterTests.createRandom());
    }

    @Override
    protected PutFilterResponse doParseInstance(XContentParser parser) throws IOException {
        return PutFilterResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
