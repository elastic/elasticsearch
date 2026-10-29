/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;

public class HttpUtilsTests extends ESTestCase {

    public void testAddDateHeader() {
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);

        // This date verifies zero-padded days, abbreviated weekday and month names, 24-hour time, and the GMT suffix.
        HttpUtils.addDateHeader(response, Instant.parse("2026-08-04T20:34:56Z"));

        assertEquals("Tue, 04 Aug 2026 20:34:56 GMT", response.headers().get(HttpUtils.DATE).getFirst());
    }

    public void testAddDateHeaderDoesNotOverwriteExistingHeader() {
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);
        response.addHeader(HttpUtils.DATE, "existing-date");

        HttpUtils.addDateHeader(response, Instant.parse("2026-08-24T12:34:56Z"));

        assertEquals("existing-date", response.headers().get(HttpUtils.DATE).getFirst());
        assertEquals(1, response.headers().get(HttpUtils.DATE).size());
    }
}
