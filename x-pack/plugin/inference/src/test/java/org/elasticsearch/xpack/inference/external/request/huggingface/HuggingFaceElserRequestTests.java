/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.huggingface;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HuggingFaceElserRequestTests extends ESTestCase {
    @SuppressWarnings("unchecked")
    public void testCreateRequest() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abc");
        var httpRequest = huggingFaceRequest.createRequest();

        assertThat(httpRequest, instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest;

        assertThat(httpPost.getURI().toString(), is("www.google.com"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), is(1));
        assertThat(requestMap.get("inputs"), instanceOf(List.class));
        var inputList = (List<String>) requestMap.get("inputs");
        assertThat(inputList, contains("abc"));
    }

    public static HuggingFaceElserRequest createRequest(String url, String apiKey, String input) throws URISyntaxException {
        var account = new HuggingFaceAccount(new URI(url), new SecureString(apiKey.toCharArray()));
        var entity = new HuggingFaceElserRequestEntity(List.of(input));

        return new HuggingFaceElserRequest(account, entity);
    }
}
