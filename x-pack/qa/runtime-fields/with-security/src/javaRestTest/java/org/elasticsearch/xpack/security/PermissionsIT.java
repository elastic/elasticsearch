/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class PermissionsIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testDLS() throws IOException {
        Request createIndex = new Request("PUT", "/dls");
        createIndex.setJsonEntity("""
            {
                "mappings" : {
                    "runtime" : {
                        "year" : {
                          "type" : "keyword",\s
                          "script" : "emit(doc['date'].value.substring(0,4))"
                        }
                    },
                    "properties" : {
                        "date" : {"type" : "keyword"}
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(createIndex));

        Request indexDoc1 = new Request("PUT", "/dls/_doc/1");
        indexDoc1.setJsonEntity("""
            {
                "date" : "2009-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc1));

        Request indexDoc2 = new Request("PUT", "/dls/_doc/2");
        indexDoc2.setJsonEntity("""
            {
                "date" : "2016-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc2));

        Request indexDoc3 = new Request("PUT", "/dls/_doc/3");
        indexDoc3.addParameter("refresh", "true");
        indexDoc3.setJsonEntity("""
            {
                "date" : "2018-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc3));

        Request searchRequest = new Request(HttpPost.METHOD_NAME, "dls/_search");
        {
            Response searchResponse = adminClient().performRequest(searchRequest);
            assertThat(ObjectPath.createFromResponse(searchResponse).evaluate("hits.total.value"), equalTo(3));
        }
        {
            Response searchResponse = client().performRequest(searchRequest);
            assertThat(ObjectPath.createFromResponse(searchResponse).evaluate("hits.total.value"), equalTo(1));
        }
    }

    public void testFLSProtectsData() throws IOException {
        Request createIndex = new Request("PUT", "/fls");
        createIndex.setJsonEntity("""
            {
                "mappings" : {
                    "runtime" : {
                        "hidden_values_count" : {
                          "type" : "long",\s
                          "script" : "emit(doc['hidden'].size())"
                        }
                    },
                    "properties" : {
                        "hidden" : {"type" : "keyword"}
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(createIndex));

        Request indexDoc1 = new Request("PUT", "/fls/_doc/1");
        indexDoc1.setJsonEntity("""
            {
                "hidden" : "should not be read"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc1));

        Request indexDoc2 = new Request("PUT", "/fls/_doc/2");
        indexDoc2.setJsonEntity("""
            {
                "hidden" : "should not be read"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc2));

        Request indexDoc3 = new Request("PUT", "/fls/_doc/3");
        indexDoc3.addParameter("refresh", "true");
        indexDoc3.setJsonEntity("""
            {
                "hidden" : "should not be read"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc3));

        Request searchRequest = new Request(HttpPost.METHOD_NAME, "fls/_search");
        searchRequest.setJsonEntity("""
            {
                "docvalue_fields" : ["hidden_values_count"]
            }
            """);
        {
            Response searchResponse = adminClient().performRequest(searchRequest);
            ObjectPath path = ObjectPath.createFromResponse(searchResponse);
            assertThat(path.evaluate("hits.total.value"), equalTo(3));
            List<Map<String, ?>> hits = path.evaluate("hits.hits");
            for (Map<String, ?> hit : hits) {
                Map<String, ?> fields = ObjectPath.evaluate(hit, "fields");
                assertThat(fields.size(), equalTo(1));
                assertThat(ObjectPath.evaluate(fields, "hidden_values_count"), equalTo(List.of(1)));
            }
        }
        {
            Response searchResponse = client().performRequest(searchRequest);
            ObjectPath path = ObjectPath.createFromResponse(searchResponse);
            assertThat(path.evaluate("hits.total.value"), equalTo(3));
            List<Map<String, ?>> hits = path.evaluate("hits.hits");
            for (Map<String, ?> hit : hits) {
                assertThat(ObjectPath.evaluate(hit, "fields.hidden_values_count"), equalTo(List.of(0)));
            }
        }
    }

    public void testFLSOnRuntimeField() throws IOException {
        Request createIndex = new Request("PUT", "/fls");
        createIndex.setJsonEntity("""
            {
                "mappings" : {
                    "runtime" : {
                        "year" : {
                          "type" : "keyword",\s
                          "script" : "emit(doc['date'].value.substring(0,4))"
                        }
                    },
                    "properties" : {
                        "date" : {"type" : "keyword"}
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(createIndex));

        Request indexDoc1 = new Request("PUT", "/fls/_doc/1");
        indexDoc1.setJsonEntity("""
            {
                "date" : "2009-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc1));

        Request indexDoc2 = new Request("PUT", "/fls/_doc/2");
        indexDoc2.setJsonEntity("""
            {
                "date" : "2016-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc2));

        Request indexDoc3 = new Request("PUT", "/fls/_doc/3");
        indexDoc3.addParameter("refresh", "true");
        indexDoc3.setJsonEntity("""
            {
                "date" : "2018-11-15T14:12:12"
            }
            """);
        assertOK(adminClient().performRequest(indexDoc3));

        // There is no FLS directly on runtime fields
        Request searchRequest = new Request(HttpPost.METHOD_NAME, "fls/_search");
        searchRequest.setJsonEntity("""
            {
                "docvalue_fields" : ["year"]
            }
            """);
        Response searchResponse = client().performRequest(searchRequest);
        ObjectPath path = ObjectPath.createFromResponse(searchResponse);
        assertThat(path.evaluate("hits.total.value"), equalTo(3));
        List<Map<String, ?>> hits = path.evaluate("hits.hits");
        for (Map<String, ?> hit : hits) {
            Map<String, ?> fields = ObjectPath.evaluate(hit, "fields");
            assertThat(fields.size(), equalTo(1));
            String id = ObjectPath.evaluate(hit, "_id");
            switch (id) {
                case "1" -> assertThat(ObjectPath.evaluate(fields, "year"), equalTo(List.of("2009")));
                case "2" -> assertThat(ObjectPath.evaluate(fields, "year"), equalTo(List.of("2016")));
                case "3" -> assertThat(ObjectPath.evaluate(fields, "year"), equalTo(List.of("2018")));
                default -> throw new UnsupportedOperationException();
            }
        }

        {
            Request fieldCapsRequest = new Request(HttpGet.METHOD_NAME, "fls/_field_caps");
            fieldCapsRequest.addParameter("fields", "year");
            Response fieldCapsResponse = adminClient().performRequest(fieldCapsRequest);
            assertThat(ObjectPath.createFromResponse(fieldCapsResponse).evaluate("fields.year"), notNullValue());
        }
        {
            // Though field_caps filters runtime fields out like ordinary fields
            Request fieldCapsRequest = new Request(HttpGet.METHOD_NAME, "fls/_field_caps");
            fieldCapsRequest.addParameter("fields", "year");
            Response fieldCapsResponse = client().performRequest(fieldCapsRequest);
            assertThat(ObjectPath.createFromResponse(fieldCapsResponse).evaluate("fields"), aMapWithSize(0));
        }
    }

    public void testPainlessExecuteWithIndexRequiresReadPrivileges() throws IOException {
        Request createIndex = new Request("PUT", "/fls");
        createIndex.setJsonEntity("""
            {
                "mappings" : {
                    "properties" : {
                        "@timestamp" : {"type" : "date"}
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(createIndex));

        Request painlessExecute = new Request("POST", "/_scripts/painless/_execute");
        painlessExecute.setJsonEntity("""
            {
              "script": {
                "source": "emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT));"
              },
              "context": "keyword_field",
              "context_setup": {
                "index": "fls",
                "document": {
                  "@timestamp": "2020-04-30T14:31:43-05:00"
                }
              }
            }
            """);
        Response response = client().performRequest(painlessExecute);
        assertOK(response);
        assertThat(EntityUtils.toString(response.getEntity()), containsString("Thursday"));
    }

    public void testPainlessExecuteWithoutIndexRequiresClusterPrivileges() {
        Request painlessExecute = new Request("POST", "/_scripts/painless/_execute");
        painlessExecute.setJsonEntity("""
            {
              "script": {
                "source": "params.count / params.total",
                "params": {
                  "count": 100.0,
                  "total": 1000.0
                }
              }
            }
            """);
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(painlessExecute));
        assertEquals(403, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(
            responseException.getMessage(),
            containsString(
                "action [cluster:admin/scripts/painless/execute] is "
                    + "unauthorized for user [test] with effective roles [test]"
                    + ", this action is granted by the cluster privileges [manage,all]\"}]"
            )
        );
    }
}
