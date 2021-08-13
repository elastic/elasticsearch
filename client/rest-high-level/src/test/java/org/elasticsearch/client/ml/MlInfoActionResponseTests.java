/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MlInfoActionResponseTests extends AbstractResponseTestCase<Response, MlInfoResponse> {

    @Override
    protected Response createServerTestInstance(XContentType xContentType) {
        int size = randomInt(10);
        Map<String, Object> info = new HashMap<>();
        for (int j = 0; j < size; j++) {
            info.put(randomAlphaOfLength(20), randomAlphaOfLength(20));
        }
        return new Response(info);
    }

    @Override
    protected MlInfoResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return MlInfoResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(Response serverTestInstance, MlInfoResponse clientInstance) {
        assertThat(serverTestInstance.getInfo(), equalTo(clientInstance.getInfo()));
    }
}
