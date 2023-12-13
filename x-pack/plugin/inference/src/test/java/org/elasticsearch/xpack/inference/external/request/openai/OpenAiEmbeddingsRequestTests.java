/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequest.buildDefaultUri;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_WithUrlOrganizationUserDefined() throws URISyntaxException, IOException {
        var request = createRequest("www.google.com", "org", "secret", "abc", "model", "user");
        var httpRequest = request.createRequest();

        assertThat(httpRequest, instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest;

        assertThat(httpPost.getURI().toString(), is("www.google.com"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(ORGANIZATION_HEADER).getValue(), is("org"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of("abc")));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("user"), is("user"));
    }

    public void testCreateRequest_WithDefaultUrl() throws URISyntaxException, IOException {
        var request = createRequest(null, "org", "secret", "abc", "model", "user");
        var httpRequest = request.createRequest();

        assertThat(httpRequest, instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest;

        assertThat(httpPost.getURI().toString(), is(buildDefaultUri().toString()));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(ORGANIZATION_HEADER).getValue(), is("org"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of("abc")));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("user"), is("user"));
    }

    public void testCreateRequest_WithDefaultUrlAndWithoutUserOrganization() throws URISyntaxException, IOException {
        var request = createRequest(null, null, "secret", "abc", "model", null);
        var httpRequest = request.createRequest();

        assertThat(httpRequest, instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest;

        assertThat(httpPost.getURI().toString(), is(buildDefaultUri().toString()));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertNull(httpPost.getLastHeader(ORGANIZATION_HEADER));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("abc")));
        assertThat(requestMap.get("model"), is("model"));
    }

    public static OpenAiEmbeddingsRequest createRequest(
        @Nullable String url,
        @Nullable String org,
        String apiKey,
        String input,
        String model,
        @Nullable String user
    ) throws URISyntaxException {
        var uri = url == null ? null : new URI(url);

        var account = new OpenAiAccount(uri, org, new SecureString(apiKey.toCharArray()));
        var entity = new OpenAiEmbeddingsRequestEntity(List.of(input), model, user);

        return new OpenAiEmbeddingsRequest(account, entity);
    }
}
