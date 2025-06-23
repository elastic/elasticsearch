/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class RestBulkActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testBulkIndexWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"index\",}";
        var sourceEscaped = "{\\\"field\\\": \\\"index\\\",}";

        var request = new Request("PUT", "/test_index/_bulk");
        request.setJsonEntity(Strings.format("{\"index\":{\"_id\":\"1\"}}\n%s\n", source));

        Response response = getRestClient().performRequest(request);
        String responseContent = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        assertThat(responseContent, containsString(sourceEscaped));

        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");

        response = getRestClient().performRequest(request);
        responseContent = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        assertThat(
            responseContent,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testBulkUpdateWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"index\",}";
        var sourceEscaped = "{\\\"field\\\": \\\"index\\\",}";

        var request = new Request("PUT", "/test_index/_bulk");
        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");
        request.setJsonEntity(Strings.format("{\"update\":{\"_id\":\"1\"}}\n{\"doc\":%s}}\n", source));

        // note: this behavior is not consistent with bulk index actions
        // In case of updates by doc, the source is eagerly parsed and will fail the entire request if it cannot be parsed
        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));

        assertThat(
            response,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }
}
