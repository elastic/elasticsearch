/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsAction.Response;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfigTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetDataFrameTransformsActionResponseTests extends ESTestCase {

    public void testInvalidTransforms() throws IOException {
        List<DataFrameTransformConfig> transforms = new ArrayList<>();

        transforms.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomInvalidDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomInvalidDataFrameTransformConfig());

        Response r = new Response(transforms);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();
        assertEquals(2, XContentMapValues.extractValue("invalid_transforms.count", responseAsMap));
        List<String> expectedInvalidTransforms = new ArrayList<>();
        expectedInvalidTransforms.add(transforms.get(1).getId());
        expectedInvalidTransforms.add(transforms.get(3).getId());
        assertEquals(expectedInvalidTransforms, XContentMapValues.extractValue("invalid_transforms.transforms", responseAsMap));
        assertWarnings(LoggerMessageFormat.format(Response.INVALID_TRANSFORMS_DEPRECATION_WARNING, 2));
    }
}
