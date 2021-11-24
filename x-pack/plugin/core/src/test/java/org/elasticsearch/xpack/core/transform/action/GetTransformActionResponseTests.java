/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetTransformActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    public static Response randomTransformResponse() {
        List<TransformConfig> configs = randomList(0, 10, () -> TransformConfigTests.randomTransformConfig());
        List<String> transformsWithoutConfig = randomBoolean() ? randomList(1, 5, () -> randomAlphaOfLengthBetween(1, 10)) : null;
        return new Response(configs, randomNonNegativeLong(), transformsWithoutConfig);
    }

    public void testInvalidTransforms() throws IOException {
        List<TransformConfig> transforms = List.of(
            TransformConfigTests.randomTransformConfig("valid-transform-1"),
            TransformConfigTests.randomInvalidTransformConfig("invalid-transform-1"),
            TransformConfigTests.randomTransformConfig("valid-transform-2"),
            TransformConfigTests.randomInvalidTransformConfig("invalid-transform-2")
        );

        Response r = new Response(transforms, transforms.size(), null);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();
        assertEquals(2, XContentMapValues.extractValue("invalid_transforms.count", responseAsMap));
        List<String> expectedInvalidTransforms = List.of("invalid-transform-1", "invalid-transform-2");
        assertEquals(expectedInvalidTransforms, XContentMapValues.extractValue("invalid_transforms.transforms", responseAsMap));
        assertWarnings(LoggerMessageFormat.format(Response.INVALID_TRANSFORMS_DEPRECATION_WARNING, 2));
    }

    public void testTransformsWithoutConfig() throws IOException {
        List<String> transformsWithoutConfig = List.of("transform-1", "transform-2", "transform-3");

        Response r = new Response(List.of(), 0, transformsWithoutConfig);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();
        assertThat(XContentMapValues.extractValue("invalid_transforms.count", responseAsMap), is(equalTo(3)));
        assertThat(
            XContentMapValues.extractValue("invalid_transforms.transforms", responseAsMap),
            is(equalTo(List.of("transform-1", "transform-2", "transform-3")))
        );
        assertWarnings(LoggerMessageFormat.format(Response.INVALID_TRANSFORMS_DEPRECATION_WARNING, 3));
    }

    public void testBothInvalidConfigsAndTransformsWithoutConfig() throws IOException {
        List<TransformConfig> transforms = List.of(TransformConfigTests.randomInvalidTransformConfig("invalid-transform-7"));
        List<String> transformsWithoutConfig = List.of("transform-1", "transform-2", "transform-3");

        Response r = new Response(transforms, transforms.size(), transformsWithoutConfig);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        r.toXContent(builder, XContent.EMPTY_PARAMS);
        Map<String, Object> responseAsMap = createParser(builder).map();
        assertThat(XContentMapValues.extractValue("invalid_transforms.count", responseAsMap), is(equalTo(4)));
        assertThat(
            XContentMapValues.extractValue("invalid_transforms.transforms", responseAsMap),
            is(equalTo(List.of("invalid-transform-7", "transform-1", "transform-2", "transform-3")))
        );
        assertWarnings(LoggerMessageFormat.format(Response.INVALID_TRANSFORMS_DEPRECATION_WARNING, 4));
    }

    @SuppressWarnings("unchecked")
    public void testNoHeaderInResponse() throws IOException {
        List<TransformConfig> transforms = randomList(0, 10, () -> TransformConfigTests.randomTransformConfig());

        Response r = new Response(transforms, transforms.size(), null);
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
                ((List<String>) XContentMapValues.extractValue("source.index", transformsResponse.get(i))).toArray(new String[0])
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
