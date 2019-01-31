/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction.Request;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.junit.Before;

public class PutDataFrameAnalyticsActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String id;

    @Before
    public void setUpId() {
        id = DataFrameAnalyticsConfigTests.randomValidId();
    }

    @Override
    protected Request createTestInstance() {
        return new Request(DataFrameAnalyticsConfigTests.createRandom(id));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(id, parser);
    }
}
