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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class RequestTests extends ESTestCase {

    public void testPing() {
        Request request = Request.ping();
        assertEquals("/", request.endpoint);
        assertEquals(0, request.params.size());
        assertNull(request.entity);
        assertEquals("HEAD", request.method);
    }

    public void testGet() {
        getAndExistsTest(Request::get, "GET");
    }

    public void testExists() {
        getAndExistsTest(Request::exists, "HEAD");
    }

    private static void getAndExistsTest(Function<GetRequest, Request> requestConverter, String method) {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        String id = randomAsciiOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                String preference = randomAsciiOfLengthBetween(3, 10);
                getRequest.preference(preference);
                expectedParams.put("preference", preference);
            }
            if (randomBoolean()) {
                String routing = randomAsciiOfLengthBetween(3, 10);
                getRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                boolean realtime = randomBoolean();
                getRequest.realtime(realtime);
                if (realtime == false) {
                    expectedParams.put("realtime", "false");
                }
            }
            if (randomBoolean()) {
                boolean refresh = randomBoolean();
                getRequest.refresh(refresh);
                if (refresh) {
                    expectedParams.put("refresh", "true");
                }
            }
            if (randomBoolean()) {
                long version = randomLong();
                getRequest.version(version);
                if (version != Versions.MATCH_ANY) {
                    expectedParams.put("version", Long.toString(version));
                }
            }
            if (randomBoolean()) {
                VersionType versionType = randomFrom(VersionType.values());
                getRequest.versionType(versionType);
                if (versionType != VersionType.INTERNAL) {
                    expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
                }
            }
            if (randomBoolean()) {
                int numStoredFields = randomIntBetween(1, 10);
                String[] storedFields = new String[numStoredFields];
                StringBuilder storedFieldsParam = new StringBuilder();
                for (int i = 0; i < numStoredFields; i++) {
                    String storedField = randomAsciiOfLengthBetween(3, 10);
                    storedFields[i] = storedField;
                    storedFieldsParam.append(storedField);
                    if (i < numStoredFields - 1) {
                        storedFieldsParam.append(",");
                    }
                }
                getRequest.storedFields(storedFields);
                expectedParams.put("stored_fields", storedFieldsParam.toString());
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    boolean fetchSource = randomBoolean();
                    getRequest.fetchSourceContext(new FetchSourceContext(fetchSource));
                    if (fetchSource == false) {
                        expectedParams.put("_source", "false");
                    }
                } else {
                    int numIncludes = randomIntBetween(0, 5);
                    String[] includes = new String[numIncludes];
                    StringBuilder includesParam = new StringBuilder();
                    for (int i = 0; i < numIncludes; i++) {
                        String include = randomAsciiOfLengthBetween(3, 10);
                        includes[i] = include;
                        includesParam.append(include);
                        if (i < numIncludes - 1) {
                            includesParam.append(",");
                        }
                    }
                    if (numIncludes > 0) {
                        expectedParams.put("_source_include", includesParam.toString());
                    }
                    int numExcludes = randomIntBetween(0, 5);
                    String[] excludes = new String[numExcludes];
                    StringBuilder excludesParam = new StringBuilder();
                    for (int i = 0; i < numExcludes; i++) {
                        String exclude = randomAsciiOfLengthBetween(3, 10);
                        excludes[i] = exclude;
                        excludesParam.append(exclude);
                        if (i < numExcludes - 1) {
                            excludesParam.append(",");
                        }
                    }
                    if (numExcludes > 0) {
                        expectedParams.put("_source_exclude", excludesParam.toString());
                    }
                    getRequest.fetchSourceContext(new FetchSourceContext(true, includes, excludes));
                }
            }
        }
        Request request = requestConverter.apply(getRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        assertEquals(expectedParams, request.params);
        assertNull(request.entity);
        assertEquals(method, request.method);
    }

    public void testIndex() throws IOException {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index, type);

        String id = randomBoolean() ? randomAsciiOfLengthBetween(3, 10) : null;
        indexRequest.id(id);

        Map<String, String> expectedParams = new HashMap<>();

        String method = "POST";
        if (id != null) {
            method = "PUT";
            if (randomBoolean()) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
        }

        // There is some logic around _create endpoint and version/version type
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            indexRequest.version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED));
            expectedParams.put("version", Long.toString(Versions.MATCH_DELETED));
        } else {
            if (randomBoolean()) {
                long version = randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, Versions.NOT_FOUND, randomNonNegativeLong());
                indexRequest.version(version);
                if (version != Versions.MATCH_ANY) {
                    expectedParams.put("version", Long.toString(version));
                }
            }
            if (randomBoolean()) {
                VersionType versionType = randomFrom(VersionType.values());
                indexRequest.versionType(versionType);
                if (versionType != VersionType.INTERNAL) {
                    expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
                }
            }
        }

        if (randomBoolean()) {
            String timeout = randomTimeValue();
            indexRequest.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", ReplicationRequest.DEFAULT_TIMEOUT.getStringRep());
        }

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAsciiOfLengthBetween(3, 10);
                indexRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String parent = randomAsciiOfLengthBetween(3, 10);
                indexRequest.parent(parent);
                expectedParams.put("parent", parent);
            }
            if (randomBoolean()) {
                String pipeline = randomAsciiOfLengthBetween(3, 10);
                indexRequest.setPipeline(pipeline);
                expectedParams.put("pipeline", pipeline);
            }

            if (randomBoolean()) {
                WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
                indexRequest.setRefreshPolicy(refreshPolicy);
                if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                    // TODO use refreshPolicy.toString() once it returns the appropriate value
                    if (refreshPolicy == WriteRequest.RefreshPolicy.IMMEDIATE) {
                        expectedParams.put("refresh", Boolean.TRUE.toString());
                    } else {
                        expectedParams.put("refresh", "wait_for");
                    }
                }
            }
        }

        XContentType xContentType = randomFrom(XContentType.values());
        int nbFields = randomIntBetween(0, 10);
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            for (int i = 0; i < nbFields; i++) {
                builder.field("field_" + i, i);
            }
            builder.endObject();
            indexRequest.source(builder);
        }

        Request request = Request.index(indexRequest);
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            assertEquals("/" + index + "/" + type + "/" + id + "/_create", request.endpoint);
        } else if (id != null) {
            assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        } else {
            assertEquals("/" + index + "/" + type, request.endpoint);
        }
        assertEquals(expectedParams, request.params);
        assertEquals(method, request.method);

        HttpEntity entity = request.entity;
        assertNotNull(entity);
        assertTrue(entity instanceof ByteArrayEntity);

        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testParams() {
        final int nbParams = randomIntBetween(0, 10);
        Request.Params params = Request.Params.builder();
        Map<String, String> expectedParams = new HashMap<>();
        for (int i = 0; i < nbParams; i++) {
            String paramName = "p_" + i;
            String paramValue = randomAsciiOfLength(5);
            params.putParam(paramName, paramValue);
            expectedParams.put(paramName, paramValue);
        }

        Map<String, String> requestParams = params.getParams();
        assertEquals(nbParams, requestParams.size());
        assertEquals(expectedParams, requestParams);
    }

    public void testParamsNoDuplicates() {
        Request.Params params = Request.Params.builder();
        params.putParam("test", "1");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> params.putParam("test", "2"));
        assertEquals("Request parameter [test] is already registered", e.getMessage());

        Map<String, String> requestParams = params.getParams();
        assertEquals(1L, requestParams.size());
        assertEquals("1", requestParams.values().iterator().next());
    }

    public void testEndpoint() {
        assertEquals("/", Request.endpoint());
        assertEquals("/", Request.endpoint(Strings.EMPTY_ARRAY));
        assertEquals("/", Request.endpoint(""));
        assertEquals("/a/b", Request.endpoint("a", "b"));
        assertEquals("/a/b/_create", Request.endpoint("a", "b", "_create"));
        assertEquals("/a/b/c/_create", Request.endpoint("a", "b", "c", "_create"));
    }
}