/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

public class RestRequestFilterTests extends ESTestCase {

    public void testFilteringItemsInSubLevels() throws IOException {
        BytesReference content = new BytesArray("{\"root\": {\"second\": {\"third\": \"password\", \"foo\": \"bar\"}}}");
        RestRequestFilter filter = () -> Collections.singleton("root.second.third");
        FakeRestRequest restRequest =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON).build();
        RestRequest filtered = filter.getFilteredRequest(restRequest);
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput()).map();
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testFilteringItemsInSubLevelsWithWildCard() throws IOException {
        BytesReference content = new BytesArray("{\"root\": {\"second\": {\"third\": \"password\", \"foo\": \"bar\"}}}");
        RestRequestFilter filter = () -> Collections.singleton("root.*.third");
        FakeRestRequest restRequest =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON).build();
        RestRequest filtered = filter.getFilteredRequest(restRequest);
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput()).map();
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testFilteringItemsInSubLevelsWithLeadingWildCard() throws IOException {
        BytesReference content = new BytesArray("{\"root\": {\"second\": {\"third\": \"password\", \"foo\": \"bar\"}}}");
        RestRequestFilter filter = () -> Collections.singleton("*.third");
        FakeRestRequest restRequest =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON).build();
        RestRequest filtered = filter.getFilteredRequest(restRequest);
        assertNotEquals(content, filtered.content());

        Map<String, Object> map = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, filtered.content().streamInput()).map();
        Map<String, Object> root = (Map<String, Object>) map.get("root");
        assertNotNull(root);
        Map<String, Object> second = (Map<String, Object>) root.get("second");
        assertNotNull(second);
        assertEquals("bar", second.get("foo"));
        assertNull(second.get("third"));
    }

    public void testRemoteAddressWorks() throws IOException {
        BytesReference content = new BytesArray("{\"root\": {\"second\": {\"third\": \"password\", \"foo\": \"bar\"}}}");
        RestRequestFilter filter = () -> Collections.singleton("*.third");
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 32768);
        FakeRestRequest restRequest =
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(content, XContentType.JSON)
                        .withRemoteAddress(address).build();
        RestRequest filtered = filter.getFilteredRequest(restRequest);
        assertEquals(address, filtered.getHttpChannel().getRemoteAddress());
    }
}
