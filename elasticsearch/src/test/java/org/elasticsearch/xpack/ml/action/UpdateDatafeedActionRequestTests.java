/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.UpdateDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.ml.datafeed.DatafeedUpdateTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.junit.Before;

public class UpdateDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String datafeedId;

    @Before
    public void setUpDatafeedId() {
        datafeedId = DatafeedConfigTests.randomValidDatafeedId();
    }

    @Override
    protected Request createTestInstance() {
        return new Request(DatafeedUpdateTests.createRandomized(datafeedId));
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
