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
package org.elasticsearch.test.rest.yaml.section;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents a test fragment that contains the information needed to call an api
 */
public class ApiCallSection {

    private final String api;
    private final Map<String, String> params = new HashMap<>();
    private final Map<String, String> headers = new HashMap<>();
    private final List<Map<String, Object>> bodies = new ArrayList<>();

    public ApiCallSection(String api) {
        this.api = api;
    }

    public String getApi() {
        return api;
    }

    public Map<String, String> getParams() {
        //make sure we never modify the parameters once returned
        return unmodifiableMap(params);
    }

    public void addParam(String key, String value) {
        String existingValue = params.get(key);
        if (existingValue != null) {
            value = existingValue + "," + value;
        }
        this.params.put(key, value);
    }

    public void addHeaders(Map<String, String> otherHeaders) {
        this.headers.putAll(otherHeaders);
    }

    public Map<String, String> getHeaders() {
        return unmodifiableMap(headers);
    }

    public List<Map<String, Object>> getBodies() {
        return Collections.unmodifiableList(bodies);
    }

    public void addBody(Map<String, Object> body) {
        this.bodies.add(body);
    }

    public boolean hasBody() {
        return bodies.size() > 0;
    }
}
