/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Contains integration tests that simulate the new indexing strategy upgrade scenarios.
 */
public class DataStreamUpgradeRestIT extends DisabledSecurityDataStreamTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password"));

    @Override
    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder();
        if (System.getProperty("tests.rest.client_path_prefix") != null) {
            builder.put(CLIENT_PATH_PREFIX, System.getProperty("tests.rest.client_path_prefix"));
        }
        return builder.put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testCompatibleMappingUpgrade() throws Exception {
        waitForLogsComponentTemplateInitialization();

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/mysql-error1");
        putPipelineRequest.setJsonEntity("{\"processors\":[]}");
        assertOK(client().performRequest(putPipelineRequest));

        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/mysql-error");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": [ "logs-mysql-*" ],
              "priority": 200,
              "composed_of": [ "logs-mappings", "logs-settings" ],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "thread_id": {
                      "type": "long"
                    }
                  }
                },
                "settings": {
                  "index.default_pipeline": "mysql-error1"
                }
              }
            }""");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Create a data stream and index first doc
        Request indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("""
            {"@timestamp": "2020-12-12","message":"abc","thread_id":23}""");
        assertOK(client().performRequest(indexRequest));

        // Create new pipeline and update default pipeline:
        putPipelineRequest = new Request("PUT", "/_ingest/pipeline/mysql-error2");
        putPipelineRequest.setJsonEntity("""
            {"processors":[{"rename":{"field":"thread_id","target_field":"thread.id","ignore_failure":true}}]}""");
        assertOK(client().performRequest(putPipelineRequest));
        Request updateSettingsRequest = new Request("PUT", "/logs-mysql-error/_settings");
        updateSettingsRequest.setJsonEntity("{ \"index\": { \"default_pipeline\" : \"mysql-error2\" }}");
        assertOK(client().performRequest(updateSettingsRequest));

        // Update template
        putComposableIndexTemplateRequest = new Request("POST", "/_index_template/mysql-error");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": [ "logs-mysql-*" ],
              "priority": 200,
              "composed_of": [ "logs-mappings", "logs-settings" ],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "thread": {
                      "properties": {
                        "id": {
                          "type": "long"
                        }
                      }
                    }
                  }
                },
                "settings": {
                  "index.default_pipeline": "mysql-error2"
                }
              }
            }""");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Update mapping
        Request putMappingRequest = new Request("PUT", "/logs-mysql-error/_mappings");
        putMappingRequest.addParameters(Map.of("write_index_only", "true"));
        putMappingRequest.setJsonEntity("""
            {"properties":{"thread":{"properties":{"id":{"type":"long"}}}}}""");
        assertOK(client().performRequest(putMappingRequest));

        // Delete old pipeline
        Request deletePipeline = new Request("DELETE", "/_ingest/pipeline/mysql-error1");
        assertOK(client().performRequest(deletePipeline));

        // Index more docs
        indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("""
            {"@timestamp": "2020-12-12","message":"abc","thread_id":24}""");
        assertOK(client().performRequest(indexRequest));
        indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("""
            {"@timestamp": "2020-12-12","message":"abc","thread":{"id":24}}""");
        assertOK(client().performRequest(indexRequest));

        Request refreshRequest = new Request("POST", "/logs-mysql-error/_refresh");
        assertOK(client().performRequest(refreshRequest));

        verifyTotalHitCount("logs-mysql-error", "{\"query\":{\"match\":{\"thread.id\": 24}}}", 2, "thread.id");

        Request deleteDateStreamRequest = new Request("DELETE", "/_data_stream/logs-mysql-error");
        assertOK(client().performRequest(deleteDateStreamRequest));
    }

    public void testConflictingMappingUpgrade() throws Exception {
        waitForLogsComponentTemplateInitialization();

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/mysql-error1");
        putPipelineRequest.setJsonEntity("{\"processors\":[]}");
        assertOK(client().performRequest(putPipelineRequest));

        // Create a template
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/mysql-error");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": [ "logs-mysql-*" ],
              "priority": 200,
              "composed_of": [ "logs-mappings", "logs-settings" ],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "thread": {
                      "type": "long"
                    }
                  }
                },
                "settings": {
                  "index.default_pipeline": "mysql-error1"
                }
              }
            }""");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Create a data stream and index first doc
        Request indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("{\"@timestamp\": \"2020-12-12\",\"message\":\"abc\",\"thread\":23}");
        assertOK(client().performRequest(indexRequest));

        // Create new pipeline and update default pipeline:
        putPipelineRequest = new Request("PUT", "/_ingest/pipeline/mysql-error2");
        putPipelineRequest.setJsonEntity("""
            {"processors":[{"rename":{"field":"thread","target_field":"thread.id","ignore_failure":true}}]}""");
        assertOK(client().performRequest(putPipelineRequest));
        Request updateSettingsRequest = new Request("PUT", "/logs-mysql-error/_settings");
        updateSettingsRequest.setJsonEntity("{ \"index\": { \"default_pipeline\" : \"mysql-error2\" }}");
        assertOK(client().performRequest(updateSettingsRequest));

        // Update template
        putComposableIndexTemplateRequest = new Request("POST", "/_index_template/mysql-error");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": [ "logs-mysql-*" ],
              "priority": 200,
              "composed_of": [ "logs-mappings", "logs-settings" ],
              "data_stream": {},
              "template": {
                "mappings": {
                  "properties": {
                    "thread": {
                      "properties": {
                        "id": {
                          "type": "long"
                        }
                      }
                    }
                  }
                },
                "settings": {
                  "index.default_pipeline": "mysql-error2"
                }
              }
            }""");
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Update mapping
        Request putMappingRequest = new Request("PUT", "/logs-mysql-error/_mappings");
        putMappingRequest.addParameters(Map.of("write_index_only", "true"));
        putMappingRequest.setJsonEntity("{\"properties\":{\"thread\":{\"properties\":{\"id\":{\"type\":\"long\"}}}}}");
        Exception e = expectThrows(ResponseException.class, () -> client().performRequest(putMappingRequest));
        assertThat(e.getMessage(), containsString("can't merge a non object mapping [thread] with an object mapping"));

        // Rollover
        Request rolloverRequest = new Request("POST", "/logs-mysql-error/_rollover");
        assertOK(client().performRequest(rolloverRequest));

        // Delete old pipeline
        Request deletePipeline = new Request("DELETE", "/_ingest/pipeline/mysql-error1");
        assertOK(client().performRequest(deletePipeline));

        // Index more docs
        indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("""
            {"@timestamp": "2020-12-12","message":"abc","thread":24}""");
        assertOK(client().performRequest(indexRequest));
        indexRequest = new Request("POST", "/logs-mysql-error/_doc");
        indexRequest.setJsonEntity("""
            {"@timestamp": "2020-12-12","message":"abc","thread":{"id":24}}""");
        assertOK(client().performRequest(indexRequest));

        Request refreshRequest = new Request("POST", "/logs-mysql-error/_refresh");
        assertOK(client().performRequest(refreshRequest));

        verifyTotalHitCount("logs-mysql-error", """
            {"query":{"match":{"thread.id": 24}}}""", 2, "thread.id");

        Request deleteDateStreamRequest = new Request("DELETE", "/_data_stream/logs-mysql-error");
        assertOK(client().performRequest(deleteDateStreamRequest));
    }

    static void verifyTotalHitCount(String index, String requestBody, int expectedTotalHits, String requiredField) throws IOException {
        Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<?, ?> responseBody = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        int totalHits = (int) XContentMapValues.extractValue("hits.total", responseBody);
        assertThat(totalHits, equalTo(expectedTotalHits));

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", responseBody);
        assertThat(hits.size(), equalTo(expectedTotalHits));
        for (Object element : hits) {
            Map<?, ?> hit = (Map<?, ?>) element;
            Object value = XContentMapValues.extractValue("_source." + requiredField, hit);
            assertThat(value, notNullValue());
        }
    }

    @SuppressWarnings("unchecked")
    private void waitForLogsComponentTemplateInitialization() throws Exception {
        assertBusy(() -> {
            try {
                Request logsComponentTemplateRequest = new Request("GET", "/_component_template/logs-*");
                Response response = client().performRequest(logsComponentTemplateRequest);
                assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

                Map<?, ?> responseBody = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    EntityUtils.toString(response.getEntity()),
                    false
                );
                List<?> componentTemplates = (List<?>) responseBody.get("component_templates");
                assertThat(componentTemplates.size(), equalTo(2));
                Set<String> names = componentTemplates.stream().map(m -> ((Map<String, String>) m).get("name")).collect(Collectors.toSet());
                assertThat(names, containsInAnyOrder("logs-mappings", "logs-settings"));
            } catch (ResponseException responseException) {
                // Retry in case of a 404, maybe they haven't been initialized yet.
                if (responseException.getResponse().getStatusLine().getStatusCode() == 404) {
                    fail();
                }
                // Throw the exception, if it was an error we did not anticipate
                throw responseException;
            }
        });
    }
}
