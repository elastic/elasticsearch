/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetTransformActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    public static Response randomTransformResponse() {
        List<TransformConfig> configs = new ArrayList<>();
        int totalConfigs = randomInt(10);
        for (int i = 0; i < totalConfigs; ++i) {
            configs.add(TransformConfigTests.randomTransformConfig());
        }

        return new Response(configs, randomNonNegativeLong());
    }

    public void testInvalidTransforms() throws IOException {
        List<TransformConfig> transforms = new ArrayList<>();

        transforms.add(TransformConfigTests.randomTransformConfig());
        transforms.add(TransformConfigTests.randomInvalidTransformConfig());
        transforms.add(TransformConfigTests.randomTransformConfig());
        transforms.add(TransformConfigTests.randomInvalidTransformConfig());

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
        List<TransformConfig> transforms = new ArrayList<>();
        int totalConfigs = randomInt(10);

        for (int i = 0; i < totalConfigs; ++i) {
            transforms.add(TransformConfigTests.randomTransformConfig());
        }

        Response r = new Response(transforms, transforms.size());
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> transformsResponse = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "transforms",
            responseAsMap
        );

        assertEquals(transforms.size(), transformsResponse.size());
        for (int i = 0; i < transforms.size(); ++i) {
            assertArrayEquals(
                transforms.get(i).getSource().getIndex(),
                ((ArrayList<String>) XContentMapValues.extractValue("source.index", transformsResponse.get(i))).toArray(new String[0])
            );
            assertEquals(null, XContentMapValues.extractValue("headers", transformsResponse.get(i)));
        }
    }

    @Override
    protected Response createTestInstance() {
        return randomTransformResponse();
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }
}
