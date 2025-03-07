/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.custom;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.hamcrest.MatcherAssert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CustomRequestTests extends ESTestCase {
    public void testCreateRequest() throws IOException {
        // create request
        var request = createRequest(null, List.of("abc"), CustomModelTests.getTestModel());
        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        String queryStringRes = "?query=" + CustomModelTests.taskSettingsValue;
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var uri = httpPost.getURI().toString();
        MatcherAssert.assertThat(uri, is(CustomModelTests.url + CustomModelTests.path + queryStringRes));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(),
            is(CustomModelTests.secretSettingsValue)
        );

        String requestBody = convertStreamToString(httpPost.getEntity().getContent());
        MatcherAssert.assertThat(requestBody, is("\"input\":\"[\"abc\"]\""));
    }

    public static CustomRequest createRequest(String query, List<String> input, CustomModel model) {
        return new CustomRequest(query, input, model);
    }

    private static String convertStreamToString(InputStream inputStream) {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return stringBuilder.toString();
    }
}
