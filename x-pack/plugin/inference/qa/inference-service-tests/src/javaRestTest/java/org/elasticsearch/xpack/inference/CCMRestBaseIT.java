/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;

import java.io.IOException;

import static org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse.ENABLED_FIELD_NAME;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.hamcrest.Matchers.instanceOf;

public class CCMRestBaseIT extends ESRestTestCase {

    static final PutCCMConfigurationAction.Request ENABLE_CCM_REQUEST = PutCCMConfigurationAction.Request.createEnabled(
        "key",
        TimeValue.THIRTY_SECONDS,
        TimeValue.THIRTY_SECONDS
    );

    static final String PUT_METHOD = "PUT";
    static final String GET_METHOD = "GET";
    static final String DELETE_METHOD = "DELETE";

    static CCMEnabledActionResponse putCCMConfiguration(PutCCMConfigurationAction.Request request) throws IOException {
        return assertSuccessAndParseResponse(putRawRequest(INFERENCE_CCM_PATH, request));
    }

    private static CCMEnabledActionResponse assertSuccessAndParseResponse(Response response) throws IOException {
        assertStatusOkOrCreated(response);
        var responseAsMap = entityAsMap(response);

        var enabled = responseAsMap.get(ENABLED_FIELD_NAME);
        assertThat(enabled, instanceOf(Boolean.class));
        return new CCMEnabledActionResponse((Boolean) enabled);
    }

    static Response putRawRequest(String endpoint, PutCCMConfigurationAction.Request actionRequest) throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        actionRequest.toXContent(builder, null);

        return putRawRequest(endpoint, Strings.toString(builder));
    }

    static Response putRawRequest(String endpoint, String requestBody) throws IOException {
        var request = new Request(PUT_METHOD, endpoint);
        request.setJsonEntity(requestBody);
        return client().performRequest(request);
    }

    static CCMEnabledActionResponse deleteCCMConfiguration() throws IOException {
        var deleteResponse = client().performRequest(new Request(DELETE_METHOD, INFERENCE_CCM_PATH));
        return assertSuccessAndParseResponse(deleteResponse);
    }

    static CCMEnabledActionResponse getCCMConfiguration() throws IOException {
        var getRequest = new Request(GET_METHOD, INFERENCE_CCM_PATH);
        var getResponse = client().performRequest(getRequest);
        return assertSuccessAndParseResponse(getResponse);
    }
}
