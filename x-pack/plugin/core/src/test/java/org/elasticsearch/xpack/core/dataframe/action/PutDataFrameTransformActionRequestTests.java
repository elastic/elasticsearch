/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests;
import org.junit.Before;

import java.io.IOException;

public class PutDataFrameTransformActionRequestTests extends AbstractSerializingDataFrameTestCase<Request> {
    private String transformId;

    @Before
    public void setupTransformId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.fromXContent(parser, transformId);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        DataFrameTransformConfig config = DataFrameTransformConfigTests.randomDataFrameTransformConfigWithoutHeaders(transformId);
        return new Request(config);
    }
}
