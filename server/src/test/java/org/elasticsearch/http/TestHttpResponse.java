/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
