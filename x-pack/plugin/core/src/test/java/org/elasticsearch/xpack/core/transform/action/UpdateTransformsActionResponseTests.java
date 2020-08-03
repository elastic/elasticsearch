/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.action.compat.UpdateTransformActionPre78;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.io.IOException;

public class UpdateTransformsActionResponseTests extends AbstractSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(TransformConfigTests.randomTransformConfigWithoutHeaders());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::fromStreamWithBWC;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return new Response(TransformConfig.fromXContent(parser, null, false));
    }

    public void testBWCPre78() throws IOException {
        Response newResponse = new Response(
            TransformConfigTests.randomTransformConfigWithoutHeaders(Version.V_7_8_0, randomAlphaOfLengthBetween(1, 10))
        );
        UpdateTransformActionPre78.Response oldResponse = writeAndReadBWCObject(
            newResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            UpdateTransformActionPre78.Response::new,
            Version.V_7_7_0
        );
        assertEquals(newResponse.getConfig().getDescription(), oldResponse.getConfig().getDescription());
        assertEquals(newResponse.getConfig().getId(), oldResponse.getConfig().getId());
        assertEquals(newResponse.getConfig().getCreateTime(), oldResponse.getConfig().getCreateTime());
        assertEquals(newResponse.getConfig().getDestination(), oldResponse.getConfig().getDestination());
        assertEquals(newResponse.getConfig().getFrequency(), oldResponse.getConfig().getFrequency());
        assertEquals(newResponse.getConfig().getPivotConfig(), oldResponse.getConfig().getPivotConfig());
        assertEquals(newResponse.getConfig().getSource(), oldResponse.getConfig().getSource());
        assertEquals(newResponse.getConfig().getSyncConfig(), oldResponse.getConfig().getSyncConfig());
        assertEquals(newResponse.getConfig().getVersion(), oldResponse.getConfig().getVersion());

        //
        Response newRequestFromOld = writeAndReadBWCObject(
            oldResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            Response::fromStreamWithBWC,
            Version.V_7_7_0
        );

        assertEquals(newResponse.getConfig().getDescription(), newRequestFromOld.getConfig().getDescription());
        assertEquals(newResponse.getConfig().getId(), newRequestFromOld.getConfig().getId());
        assertEquals(newResponse.getConfig().getCreateTime(), newRequestFromOld.getConfig().getCreateTime());
        assertEquals(newResponse.getConfig().getDestination(), newRequestFromOld.getConfig().getDestination());
        assertEquals(newResponse.getConfig().getFrequency(), newRequestFromOld.getConfig().getFrequency());
        assertEquals(newResponse.getConfig().getPivotConfig(), newRequestFromOld.getConfig().getPivotConfig());
        assertEquals(newResponse.getConfig().getSource(), newRequestFromOld.getConfig().getSource());
        assertEquals(newResponse.getConfig().getSyncConfig(), newRequestFromOld.getConfig().getSyncConfig());
        assertEquals(newResponse.getConfig().getVersion(), newRequestFromOld.getConfig().getVersion());
    }
}
