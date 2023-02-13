/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RestRequestFilterTests extends ESTestCase {

    public void testFilteringItemsInSubLevels() throws IOException {
        BytesReference content = new BytesArray("""
            {"root": {"second": {"third": "password", "foo": "bar"}}}""");
        RestRequestFilter filter = () -> Collections.singleton("root.second.third");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON)
            .build();
        HttpRequest filtered = filter.formatRequestContentForAuditing(restRequest.getHttpRequest(), restRequest.getXContentType());
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput())
            .map();
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        @SuppressWarnings("unchecked")
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testFilteringItemsInSubLevelsWithWildCard() throws IOException {
        BytesReference content = new BytesArray("""
            {"root": {"second": {"third": "password", "foo": "bar"}}}""");
        RestRequestFilter filter = () -> Collections.singleton("root.*.third");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON)
            .build();
        HttpRequest filtered = filter.formatRequestContentForAuditing(restRequest.getHttpRequest(), restRequest.getXContentType());
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput())
            .map();
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        @SuppressWarnings("unchecked")
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testFilteringItemsInSubLevelsWithLeadingWildCard() throws IOException {
        BytesReference content = new BytesArray("""
            {"root": {"second": {"third": "password", "foo": "bar"}}}""");
        RestRequestFilter filter = () -> Collections.singleton("*.third");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON)
            .build();
        HttpRequest filtered = filter.formatRequestContentForAuditing(restRequest.getHttpRequest(), restRequest.getXContentType());
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput())
            .map();
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        @SuppressWarnings("unchecked")
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testFilterUnknownContentTypeThrows() throws IOException {
        RestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("""
            {"simple": "test"}"""), null)
            .withPath("/whatever")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("foo/bar")))
            .build();
        if (randomBoolean()) {
            restRequest = new TestRestRequest(restRequest);
        }
        RestRequestFilter filter = () -> Collections.singleton("root.second.third");
        HttpRequest filtered = filter.formatRequestContentForAuditing(restRequest.getHttpRequest(), restRequest.getXContentType());
        NullPointerException e = expectThrows(NullPointerException.class, () -> filtered.content());
        assertThat(e.getMessage(), containsString("unknown content type"));
    }

    private static class TestRestRequest extends RestRequest {
        TestRestRequest(RestRequest other) {
            super(other);
        }
    }
}
