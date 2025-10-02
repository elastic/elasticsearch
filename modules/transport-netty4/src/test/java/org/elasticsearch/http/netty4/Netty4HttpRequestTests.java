/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeHttpBodyStream;

public class Netty4HttpRequestTests extends ESTestCase {

    public void testEmptyFullContent() {
        final var request = new Netty4HttpRequest(0, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"), HttpBody.empty());
        assertFalse(request.hasContent());
    }

    public void testEmptyStreamContent() {
        final var request = new Netty4HttpRequest(
            0,
            new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"),
            new FakeHttpBodyStream()
        );
        assertFalse(request.hasContent());
    }

    public void testNonEmptyFullContent() {
        final var len = between(1, 1024);
        final var request = new Netty4HttpRequest(
            0,
            new DefaultHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/",
                new DefaultHttpHeaders().add(HttpHeaderNames.CONTENT_LENGTH, len)
            ),
            HttpBody.fromBytesReference(new BytesArray(new byte[len]))
        );
        assertTrue(request.hasContent());
    }

    public void testNonEmptyStreamContent() {
        final var len = between(1, 1024);
        final var nettyRequestWithLen = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        HttpUtil.setContentLength(nettyRequestWithLen, len);
        final var requestWithLen = new Netty4HttpRequest(0, nettyRequestWithLen, new FakeHttpBodyStream());
        assertTrue(requestWithLen.hasContent());

        final var nettyChunkedRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", new DefaultHttpHeaders());
        HttpUtil.setTransferEncodingChunked(nettyChunkedRequest, true);
        final var chunkedRequest = new Netty4HttpRequest(0, nettyChunkedRequest, new FakeHttpBodyStream());
        assertTrue(chunkedRequest.hasContent());
    }

    public void testReplaceContent() {
        final var len = between(1, 1024);
        final var nettyRequestWithLen = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        HttpUtil.setContentLength(nettyRequestWithLen, len);
        final var streamRequest = new Netty4HttpRequest(0, nettyRequestWithLen, new FakeHttpBodyStream());

        streamRequest.setBody(HttpBody.fromBytesReference(randomBytesReference(len)));
        assertTrue(streamRequest.hasContent());
    }
}
