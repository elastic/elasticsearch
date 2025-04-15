/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.elasticsearch.xpack.inference.services.custom.CustomModelTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
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
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(CustomModelTests.secretSettingsValue));

        String requestBody = convertToString(httpPost.getEntity().getContent());

        MatcherAssert.assertThat(entityAsMap(httpPost.getEntity().getContent()), is(Map.of("input", List.of("abc"))));
    }

//    public void testPlaceholderValidation() {
//        CustomRequest.placeholderValidation("all substituted", randomBoolean() ? false : null);
//        CustomRequest.placeholderValidation("all substituted", true);
//
//        var e = expectThrows(
//            IllegalArgumentException.class,
//            () -> CustomRequest.placeholderValidation("contains ${a} substitution", randomBoolean() ? false : null)
//        );
//        assertThat(e.getMessage(), containsString("variable is not replaced, found placeholder in [contains ${a} substitution]"));
//        CustomRequest.placeholderValidation("contains ${a unsubstituted value} but not checked", true);
//    }

    public static CustomRequest createRequest(String query, List<String> input, CustomModel model) {
        return new CustomRequest(query, input, model);
    }

    private static String convertToString(InputStream inputStream) throws IOException {
        return XContentHelper.stripWhitespace(Streams.copyToString(new InputStreamReader(inputStream, StandardCharsets.UTF_8)));
    }
}
