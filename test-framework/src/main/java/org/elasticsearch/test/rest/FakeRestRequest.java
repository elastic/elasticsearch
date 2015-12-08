/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FakeRestRequest extends RestRequest {

    private final Map<String, String> headers;

    private final Map<String, String> params;

    private final Set<String> consumedParams;

    public FakeRestRequest() {
        this(new HashMap<String, String>(), new HashMap<String, String>());
    }

    public FakeRestRequest(Map<String, String> headers, Map<String, String> context) {
        this.headers = headers;
        for (Map.Entry<String, String> entry : context.entrySet()) {
            putInContext(entry.getKey(), entry.getValue());
        }
        this.params = new HashMap<>();
        this.consumedParams = new HashSet<>(params().size());
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
        this.consumedParams.add(key);
        return params.get(key);
    }

    @Override
    public String param(String key, String defaultValue) {
        this.consumedParams.add(key);
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public boolean allParamsConsumed() {
        return this.consumedParams.containsAll(this.params().keySet());
    }


    @Override
    public Map<String, String> params() {
        return params;
    }
}