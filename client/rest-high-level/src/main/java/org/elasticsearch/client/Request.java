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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

final class Request {

    final String method;
    final String endpoint;
    final Map<String, String> params;
    final HttpEntity entity;

    Request(String method, String endpoint, Map<String, String> params, HttpEntity entity) {
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
                ", params=" + params +
                '}';
    }

    static Request ping() {
        return new Request("HEAD", "/", Collections.emptyMap(), null);
    }

    static Request exists(GetRequest getRequest) {
        return new Request("HEAD", getEndpoint(getRequest), getParams(getRequest), null);
    }

    static Request get(GetRequest getRequest) {
        return new Request("GET", getEndpoint(getRequest), getParams(getRequest), null);
    }

    private static Map<String, String> getParams(GetRequest getRequest) {
        Params parameters = Params.builder();
        parameters.withPreference(getRequest.preference());
        parameters.withRouting(getRequest.routing());
        parameters.withParent(getRequest.parent());
        parameters.withRefresh(getRequest.refresh());
        parameters.withRealtime(getRequest.realtime());
        parameters.withStoredFields(getRequest.storedFields());
        parameters.withVersion(getRequest.version());
        parameters.withVersionType(getRequest.versionType());
        parameters.withFetchSourceContext(getRequest.fetchSourceContext());
        return parameters.getParams();
    }

    private static String getEndpoint(GetRequest getRequest) {
        StringJoiner pathJoiner = new StringJoiner("/", "/", "");
        return pathJoiner.add(getRequest.index()).add(getRequest.type()).add(getRequest.id()).toString();
    }

    static Request index(IndexRequest indexRequest) {
        String method = Strings.hasLength(indexRequest.id()) ? "PUT" : "POST";

        Endpoint endpoint = Endpoint.builder();
        endpoint.add(indexRequest.index());
        endpoint.add(indexRequest.type());
        endpoint.add(indexRequest.id());
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            endpoint.withSuffix("/_create");
        }

        Params parameters = Params.builder();
        parameters.withRouting(indexRequest.routing());
        parameters.withParent(indexRequest.parent());
        parameters.withTimeout(indexRequest.timeout());
        parameters.withVersion(indexRequest.version());
        parameters.withVersionType(indexRequest.versionType());
        parameters.withPipeline(indexRequest.getPipeline());
        parameters.withRefreshPolicy(indexRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(indexRequest.waitForActiveShards());

        BytesRef source = indexRequest.source().toBytesRef();
        ContentType contentType = ContentType.create(indexRequest.getContentType().mediaType());
        HttpEntity entity = new ByteArrayEntity(source.bytes, source.offset, source.length, contentType);

        return new Request(method, endpoint.getEndpoint(), parameters.getParams(), entity);
    }

    /**
     * Utility class to build request's endpoint.
     */
    private static class Endpoint {
        private final List<String> parts = new ArrayList<>();
        private final String delimiter = "/";
        private final String prefix = "/";
        private String suffix = "";

        private Endpoint() {
        }

        Endpoint withSuffix(String suffix) {
            this.suffix = Objects.requireNonNull(suffix, "suffix must not be null");
            return this;
        }

        Endpoint add(String part) {
            if (Strings.hasLength(part)) {
                parts.add(part);
            }
            return this;
        }

        String getEndpoint() {
            StringJoiner joiner = new StringJoiner(delimiter, prefix, suffix);
            for (String part : parts) {
                joiner.add(part);
            }
            return joiner.toString();
        }

        static Endpoint builder() {
            return new Endpoint();
        }
    }

    /**
     * Utility class to build request's parameters map and centralize all parameter names.
     */
    private static class Params {
        private final Map<String, String> params = new HashMap<>();

        private Params() {
        }

        Params putParam(String key, String value) {
            if (Strings.hasLength(value)) {
                params.put(key, value);
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
            if (fetchSourceContext != null) {
                if (fetchSourceContext.fetchSource() == false) {
                    putParam("_source", Boolean.FALSE.toString());
                }
                if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                    putParam("_source_include", String.join(",", fetchSourceContext.includes()));
                }
                if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                    putParam("_source_exclude", String.join(",", fetchSourceContext.excludes()));
                }
            }
            return this;
        }

        Params withParent(String parent) {
            return putParam("parent", parent);
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withRealtime(boolean realtime) {
            if (realtime == false) {
                return putParam("realtime", Boolean.FALSE.toString());
            }
            return this;
        }

        Params withRefresh(boolean refresh) {
            if (refresh) {
                return withRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            return this;
        }

        Params withRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                return putParam("refresh", refreshPolicy.toString());
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withStoredFields(String[] storedFields) {
            if (storedFields != null && storedFields.length > 0) {
                return putParam("stored_fields", String.join(",", storedFields));
            }
            return this;
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount) {
            if (activeShardCount != null && activeShardCount != ActiveShardCount.DEFAULT) {
                return putParam("wait_for_active_shards", activeShardCount.toString().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Map<String, String> getParams() {
            return Collections.unmodifiableMap(params);
        }

        static Params builder() {
            return new Params();
        }
    }
}
