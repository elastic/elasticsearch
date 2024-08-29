/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest.FakeHttpRequest;

import java.util.HashMap;
import java.util.List;

public class HttpUtilsTests extends ESTestCase {

    public void testContentLengthHeaderPresent() {
        var headers = new HashMap<String, List<String>>();
        headers.put("content-length", List.of("10"));
        var req = new FakeHttpRequest(RestRequest.Method.GET, "/", BytesArray.EMPTY, headers);
        assertEquals(10, org.elasticsearch.http.HttpUtils.contentLengthHeader(req));
    }

    public void testContentLengthHeaderMissing() {
        var req = new FakeHttpRequest(RestRequest.Method.GET, "/", BytesArray.EMPTY, new HashMap<>());
        assertEquals(0, HttpUtils.contentLengthHeader(req));
    }

    public void testChunkedEncodingPresent() {
        var headers = new HashMap<String, List<String>>();
        headers.put("transfer-encoding", List.of("chunked"));
        var req = new FakeHttpRequest(RestRequest.Method.GET, "/", BytesArray.EMPTY, headers);
        assertTrue(HttpUtils.isChunkedTransferEncoding(req));
    }

    public void testChunkedEncodingNotPresent() {
        var req = new FakeHttpRequest(RestRequest.Method.GET, "/", BytesArray.EMPTY, new HashMap<>());
        assertFalse(HttpUtils.isChunkedTransferEncoding(req));
    }
}
