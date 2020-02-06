/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreviewTransformsActionResponseTests extends AbstractSerializingTestCase<Response> {


    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return Response.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        int size = randomIntBetween(0, 10);
        List<Map<String, Object>> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(Map.of(randomAlphaOfLength(10), Map.of("value1", randomIntBetween(1, 100))));
        }

        Response response = new Response(data);
        if (randomBoolean()) {
            size = randomIntBetween(0, 10);
            if (randomBoolean()) {
                Map<String, Object> mappings = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    mappings.put(randomAlphaOfLength(10), Map.of("type", randomAlphaOfLength(10)));
                }
                response.setMappings(mappings);
            } else {
                Map<String, String> mappings = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    mappings.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
                }
                response.setMappingsFromStringMap(mappings);
            }
        }

        return response;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
