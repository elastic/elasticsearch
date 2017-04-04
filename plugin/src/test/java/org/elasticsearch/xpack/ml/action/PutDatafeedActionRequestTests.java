/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.junit.Before;

import java.util.Arrays;

public class PutDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String datafeedId;

    @Before
    public void setUpDatafeedId() {
        datafeedId = DatafeedConfigTests.randomValidDatafeedId();
    }

    @Override
    protected Request createTestInstance() {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, randomAlphaOfLength(10));
        datafeedConfig.setIndexes(Arrays.asList(randomAlphaOfLength(10)));
        datafeedConfig.setTypes(Arrays.asList(randomAlphaOfLength(10)));
        return new Request(datafeedConfig.build());
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(datafeedId, parser);
    }

}
