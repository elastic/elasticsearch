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
package org.elasticsearch.test.rest.section;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Represents a test fragment that contains the information needed to call an api
 */
public class ApiCallSection {

    private final String api;
    private final Map<String, String> params = Maps.newHashMap();
    private final List<Map<String, Object>> bodies = Lists.newArrayList();

    public ApiCallSection(String api) {
        this.api = api;
    }

    public String getApi() {
        return api;
    }

    public Map<String, String> getParams() {
        //make sure we never modify the parameters once returned
        return ImmutableMap.copyOf(params);
    }

    public void addParam(String key, String value) {
        String existingValue = params.get(key);
        if (existingValue != null) {
            value = Joiner.on(",").join(existingValue, value);
        }
        this.params.put(key, value);
    }

    public List<Map<String, Object>> getBodies() {
        return ImmutableList.copyOf(bodies);
    }

    public void addBody(Map<String, Object> body) {
        this.bodies.add(body);
    }

    public boolean hasBody() {
        return bodies.size() > 0;
    }
}
