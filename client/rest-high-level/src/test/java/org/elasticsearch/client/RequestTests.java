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

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class RequestTests extends ESTestCase {

    public void testPing() {
        Request request = Request.ping();
        assertEquals("/", request.endpoint);
        assertEquals(0, request.params.size());
        assertNull(request.entity);
        assertEquals("HEAD", request.method);
    }

    public void testGet() {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        String id = randomAsciiOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("ignore", "404");
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

        Request request = Request.get(getRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        assertEquals(expectedParams, request.params);
        assertNull(request.entity);
        assertEquals("GET", request.method);
    }
}