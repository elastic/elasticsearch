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

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

final class Request {

    final String method;
    final String endpoint;
    final Map<String, String> params;
    final HttpEntity entity;

    private Request(String method, String endpoint, Map<String, String> params, HttpEntity entity) {
        this.method = method;
        this.endpoint = endpoint;
        this.params = params;
        this.entity = entity;
    }

    @Override
    public String toString() {
        return "Request{" +
                "method='" + method + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }

    static Request ping() {
        return new Request("HEAD", "/", Collections.emptyMap(), null);
    }

    static Request get(GetRequest getRequest) {
        StringJoiner pathJoiner = new StringJoiner("/", "/", "");
        String endpoint = pathJoiner.add(getRequest.index()).add(getRequest.type()).add(getRequest.id()).toString();
        Map<String, String> params = new HashMap<>();
        params.put("ignore", "404");
        putParam("preference", getRequest.preference(), params);
        putParam("routing", getRequest.routing(), params);
        putParam("parent", getRequest.parent(), params);
        if (getRequest.refresh()) {
            params.put("refresh", Boolean.TRUE.toString());
        }
        if (getRequest.realtime() == false) {
            params.put("realtime", Boolean.FALSE.toString());
        }
        if (getRequest.storedFields() != null && getRequest.storedFields().length > 0) {
            params.put("stored_fields", String.join(",", getRequest.storedFields()));
        }
        if (getRequest.version() != Versions.MATCH_ANY) {
            params.put("version", Long.toString(getRequest.version()));
        }
        if (getRequest.versionType() != VersionType.INTERNAL) {
            params.put("version_type", getRequest.versionType().name().toLowerCase(Locale.ROOT));
        }
        if (getRequest.fetchSourceContext() != null) {
            FetchSourceContext fetchSourceContext = getRequest.fetchSourceContext();
            if (fetchSourceContext.fetchSource() == false) {
                params.put("_source", Boolean.FALSE.toString());
            }
            if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                params.put("_source_include", String.join(",", fetchSourceContext.includes()));
            }
            if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                params.put("_source_exclude", String.join(",", fetchSourceContext.excludes()));
            }
        }
        return new Request("GET", endpoint, params, null);
    }

    private static void putParam(String key, String value, Map<String, String> params) {
        if (Strings.hasLength(value)) {
            params.put(key, value);
        }
    }
}
