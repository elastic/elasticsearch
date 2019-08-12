/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetDataFrameTransformsActionResponseTests extends AbstractWireSerializingDataFrameTestCase<Response> {

    public void testInvalidTransforms() throws IOException {
        List<DataFrameTransformConfig> transforms = new ArrayList<>();

        transforms.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomInvalidDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        transforms.add(DataFrameTransformConfigTests.randomInvalidDataFrameTransformConfig());

        Response r = new Response(transforms, transforms.size());
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

    @SuppressWarnings("unchecked")
    public void testNoHeaderInResponse() throws IOException {
        List<DataFrameTransformConfig> transforms = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            transforms.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        }

        Response r = new Response(transforms, transforms.size());
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> transformsResponse = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms",
                responseAsMap);

        assertEquals(transforms.size(), transformsResponse.size());
        for (int i = 0; i < transforms.size(); ++i) {
            assertArrayEquals(transforms.get(i).getSource().getIndex(),
                ((ArrayList<String>)XContentMapValues.extractValue("source.index", transformsResponse.get(i))).toArray(new String[0]));
            assertEquals(null, XContentMapValues.extractValue("headers", transformsResponse.get(i)));
        }
    }

    @Override
    protected Response createTestInstance() {
        List<DataFrameTransformConfig> configs = new ArrayList<>();
        for (int i = 0; i < randomInt(10); ++i) {
            configs.add(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        }

        return new Response(configs, randomNonNegativeLong());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }
}
