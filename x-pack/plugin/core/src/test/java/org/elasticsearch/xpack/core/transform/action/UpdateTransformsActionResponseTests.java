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
        Response newResponse = createTestInstance();
        UpdateTransformActionPre78.Response oldResponse = translateBWCObject(
            newResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            UpdateTransformActionPre78.Response::new,
            Version.V_7_7_0
        );

        assertEquals(newResponse.getConfig(), oldResponse.getConfig());

        Response newRequestFromOld = translateBWCObject(
            oldResponse,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            Response::fromStreamWithBWC,
            Version.CURRENT
        );

        assertEquals(newResponse, newRequestFromOld);
    }
}
