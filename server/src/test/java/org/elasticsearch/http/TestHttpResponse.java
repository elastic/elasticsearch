/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestHttpResponse implements HttpResponse {

    private final RestStatus status;
    private final BytesReference content;
    private final Map<String, List<String>> headers = new HashMap<>();

    TestHttpResponse(RestStatus status, BytesReference content) {
        this.status = status;
        this.content = content;
    }

    public BytesReference content() {
        return content;
    }

    public RestStatus status() {
        return status;
    }

    public Map<String, List<String>> headers() {
        return headers;
    }

    @Override
    public void addHeader(String name, String value) {
        if (headers.containsKey(name) == false) {
            ArrayList<String> values = new ArrayList<>();
            values.add(value);
            headers.put(name, values);
        } else {
            headers.get(name).add(value);
        }
    }

    @Override
    public boolean containsHeader(String name) {
        return headers.containsKey(name);
    }
}
