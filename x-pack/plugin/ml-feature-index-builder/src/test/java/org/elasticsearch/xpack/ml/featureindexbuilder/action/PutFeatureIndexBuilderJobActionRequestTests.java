/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction.Request;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfigTests;
import org.junit.Before;

import java.io.IOException;

public class PutFeatureIndexBuilderJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String jobId;

    @Before
    public void setupJobID() {
        jobId = randomAlphaOfLengthBetween(1,10);
    }

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return  Request.fromXContent(parser, jobId);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        FeatureIndexBuilderJobConfig config = FeatureIndexBuilderJobConfigTests.randomFeatureIndexBuilderJobConfig();
        return new Request(config);
    }

}
