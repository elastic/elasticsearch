/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.io.IOException;

public class UpdateTransformsActionResponseTests extends AbstractSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(TransformConfigTests.randomDataFrameTransformConfigWithoutHeaders());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return new Response(TransformConfig.fromXContent(parser, null, false));
    }
}
