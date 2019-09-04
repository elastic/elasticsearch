/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests;

import java.io.IOException;

public class UpdateDataFrameTransformsActionResponseTests extends AbstractSerializingDataFrameTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(DataFrameTransformConfigTests.randomDataFrameTransformConfigWithoutHeaders());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return new Response(DataFrameTransformConfig.fromXContent(parser, null, false));
    }
}
