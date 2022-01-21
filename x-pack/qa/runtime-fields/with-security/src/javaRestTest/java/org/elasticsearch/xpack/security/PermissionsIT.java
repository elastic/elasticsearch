/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@SuppressWarnings("removal")
public class PermissionsIT extends ESRestTestCase {

    private static HighLevelClient highLevelClient;
    private static HighLevelClient adminHighLevelClient;

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

    @Before
    public void initHighLevelClient() {
        if (highLevelClient == null) {
            highLevelClient = new HighLevelClient(client());
            adminHighLevelClient = new HighLevelClient(adminClient());
        }
    }

    @AfterClass
    public static void closeHighLevelClients() throws IOException {
        highLevelClient.close();
        adminHighLevelClient.close();
        highLevelClient = null;
        adminHighLevelClient = null;
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

        SearchRequest searchRequest = new SearchRequest("dls");
        {
            SearchResponse searchResponse = adminHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(3, searchResponse.getHits().getTotalHits().value);
        }
        {
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
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

        SearchRequest searchRequest = new SearchRequest("fls").source(new SearchSourceBuilder().docValueField("hidden_values_count"));
        {
            SearchResponse searchResponse = adminHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(3, searchResponse.getHits().getTotalHits().value);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertEquals(1, hit.getFields().size());
                assertEquals(1, (int) hit.getFields().get("hidden_values_count").getValue());
            }
        }
        {
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            assertEquals(3, searchResponse.getHits().getTotalHits().value);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertEquals(0, (int) hit.getFields().get("hidden_values_count").getValue());
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
        SearchRequest searchRequest = new SearchRequest("fls").source(new SearchSourceBuilder().docValueField("year"));
        SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        assertEquals(3, searchResponse.getHits().getTotalHits().value);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, DocumentField> fields = hit.getFields();
            assertEquals(1, fields.size());
            switch (hit.getId()) {
                case "1" -> assertEquals("2009", fields.get("year").getValue().toString());
                case "2" -> assertEquals("2016", fields.get("year").getValue().toString());
                case "3" -> assertEquals("2018", fields.get("year").getValue().toString());
                default -> throw new UnsupportedOperationException();
            }
        }

        {
            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices("fls").fields("year");
            FieldCapabilitiesResponse fieldCapabilitiesResponse = adminHighLevelClient.fieldCaps(fieldCapsRequest, RequestOptions.DEFAULT);
            assertNotNull(fieldCapabilitiesResponse.get().get("year"));
        }
        {
            // Though field_caps filters runtime fields out like ordinary fields
            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices("fls").fields("year");
            FieldCapabilitiesResponse fieldCapabilitiesResponse = highLevelClient.fieldCaps(fieldCapsRequest, RequestOptions.DEFAULT);
            assertEquals(0, fieldCapabilitiesResponse.get().size());
        }
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }
}
