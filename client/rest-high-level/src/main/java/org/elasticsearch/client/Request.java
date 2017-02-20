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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

import static org.elasticsearch.common.xcontent.XContentHelper.createParser;

final class Request {

    private static final String DELIMITER = "/";

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
        Request request = get(getRequest);
        return new Request(HttpHead.METHOD_NAME, request.endpoint, request.params, null);
    }

    static Request get(GetRequest getRequest) {
        String endpoint = endpoint(getRequest.index(), getRequest.type(), getRequest.id());

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

        return new Request(HttpGet.METHOD_NAME, endpoint, parameters.getParams(), null);
    }

    static Request index(IndexRequest indexRequest) {
        String method = Strings.hasLength(indexRequest.id()) ? HttpPut.METHOD_NAME : HttpPost.METHOD_NAME;

        boolean isCreate = (indexRequest.opType() == DocWriteRequest.OpType.CREATE);
        String endpoint = endpoint(indexRequest.index(), indexRequest.type(), indexRequest.id(), isCreate ? "_create" : null);

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

        return new Request(method, endpoint, parameters.getParams(), entity);
    }

    static Request update(UpdateRequest updateRequest) {
        String endpoint = endpoint(updateRequest.index(), updateRequest.type(), updateRequest.id(), "_update");

        Params parameters = Params.builder();
        parameters.withRouting(updateRequest.routing());
        parameters.withParent(updateRequest.parent());
        parameters.withTimeout(updateRequest.timeout());
        parameters.withRefreshPolicy(updateRequest.getRefreshPolicy());
        parameters.withWaitForActiveShards(updateRequest.waitForActiveShards());
        parameters.withDocAsUpsert(updateRequest.docAsUpsert());
        parameters.withFetchSourceContext(updateRequest.fetchSource());
        parameters.withRetryOnConflict(updateRequest.retryOnConflict());
        parameters.withVersion(updateRequest.version());
        parameters.withVersionType(updateRequest.versionType());

        XContentType xContentType = null;
        if (updateRequest.doc() != null) {
            xContentType = updateRequest.doc().getContentType();
        } else if (updateRequest.upsertRequest() != null) {
            xContentType = updateRequest.upsertRequest().getContentType();
        } else {
            xContentType = Requests.INDEX_CONTENT_TYPE;
        }

        return new Request(HttpPost.METHOD_NAME, endpoint, parameters.getParams(), toHttpEntity(updateRequest, xContentType));
    }

    static ByteArrayEntity toHttpEntity(UpdateRequest updateRequest, XContentType xContentType) {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            if (updateRequest.docAsUpsert()) {
                builder.field("doc_as_upsert", updateRequest.docAsUpsert());
            }
            IndexRequest doc = updateRequest.doc();
            if (doc != null) {
                try (XContentParser parser = createParser(NamedXContentRegistry.EMPTY, doc.source(), doc.getContentType())) {
                    builder.field("doc").copyCurrentStructure(parser);
                }
            }
            Script script = updateRequest.script();
            if (script != null) {
                builder.field("script", script);
            }
            IndexRequest upsert = updateRequest.upsertRequest();
            if (upsert != null) {
                try (XContentParser parser = createParser(NamedXContentRegistry.EMPTY, upsert.source(), upsert.getContentType())) {
                    builder.field("upsert").copyCurrentStructure(parser);
                }
            }
            if (updateRequest.scriptedUpsert()) {
                builder.field("scripted_upsert", updateRequest.scriptedUpsert());
            }
            if (updateRequest.detectNoop() == false) {
                builder.field("detect_noop", updateRequest.detectNoop());
            }
            if (updateRequest.fetchSource() != null) {
                builder.field("_source", updateRequest.fetchSource());
            }
            builder.endObject();

            BytesRef requestBody = builder.bytes().toBytesRef();
            ContentType contentType = ContentType.create(xContentType.mediaType());
            return new ByteArrayEntity(requestBody.bytes, requestBody.offset, requestBody.length, contentType);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to build HTTP entity from update request", e);
        }
    }

    /**
     * Utility method to build request's endpoint.
     */
    static String endpoint(String... parts) {
        if (parts == null || parts.length == 0) {
            return DELIMITER;
        }

        StringJoiner joiner = new StringJoiner(DELIMITER, DELIMITER, "");
        for (String part : parts) {
            if (part != null) {
                joiner.add(part);
            }
        }
        return joiner.toString();
    }

    /**
     * Utility class to build request's parameters map and centralize all parameter names.
     */
    static class Params {
        private final Map<String, String> params = new HashMap<>();

        private Params() {
        }

        Params putParam(String key, String value) {
            if (Strings.hasLength(value)) {
                if (params.putIfAbsent(key, value) != null) {
                    throw new IllegalArgumentException("Request parameter [" + key + "] is already registered");
                }
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
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
                return putParam("refresh", refreshPolicy.getValue());
            }
            return this;
        }

        Params withRetryOnConflict(int retryOnConflict) {
            if (retryOnConflict > 0) {
                return putParam("retry_on_conflict", String.valueOf(retryOnConflict));
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
