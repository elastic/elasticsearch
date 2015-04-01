/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;

import java.util.HashMap;
import java.util.Map;

// TODO fix in core so it is packaged with test jar and remove
public class FakeRestRequest extends RestRequest {

    private final Map<String, String> headers;

    private final Map<String, String> params;

    public FakeRestRequest() {
        this(new HashMap<String, String>(), new HashMap<String, String>());
    }

    public FakeRestRequest(Map<String, String> headers, Map<String, String> context) {
        this.headers = headers;
        for (Map.Entry<String, String> entry : context.entrySet()) {
            putInContext(entry.getKey(), entry.getValue());
        }
        this.params = new HashMap<>();
    }

    @Override
    public Method method() {
        return Method.GET;
    }

    @Override
    public String uri() {
        return "/";
    }

    @Override
    public String rawPath() {
        return "/";
    }

    @Override
    public boolean hasContent() {
        return false;
    }

    // Don't add Override annotation as this method doesn't exist in ES 1.x branch anymore
    public boolean contentUnsafe() {
        return false;
    }

    @Override
    public BytesReference content() {
        return null;
    }

    @Override
    public String header(String name) {
        return headers.get(name);
    }

    @Override
    public Iterable<Map.Entry<String, String>> headers() {
        return headers.entrySet();
    }

    @Override
    public boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override
    public String param(String key) {
        return params.get(key);
    }

    @Override
    public String param(String key, String defaultValue) {
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Map<String, String> params() {
        return params;
    }
}
