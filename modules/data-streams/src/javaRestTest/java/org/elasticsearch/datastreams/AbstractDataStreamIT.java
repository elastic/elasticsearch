/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This base class provides the boilerplate to simplify the development of integration tests.
 * Aside from providing useful helper methods and disabling unnecessary plugins,
 * it waits until an {@linkplain  #indexTemplateName() index template} is installed, which happens asynchronously in StackTemplateRegistry.
 * This avoids race conditions leading to flaky tests by ensuring the template has been installed before executing the tests.
 */
public abstract class AbstractDataStreamIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        // Disable apm-data so the index templates it installs do not impact
        // tests such as testIgnoreDynamicBeyondLimit.
        .setting("xpack.apm_data.enabled", "false")
        .build();
    protected RestClient client;

    static void waitForIndexTemplate(RestClient client, String indexTemplate) throws Exception {
        assertBusy(() -> {
            try {
                Request request = new Request("GET", "_index_template/" + indexTemplate);
                assertOK(client.performRequest(request));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        }, 15, TimeUnit.SECONDS);
    }

    static void createDataStream(RestClient client, String name) throws IOException {
        Request request = new Request("PUT", "_data_stream/" + name);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    static String getWriteBackingIndex(RestClient client, String name) throws IOException {
        Request request = new Request("GET", "_data_stream/" + name);
        List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        List<Map<String, String>> indices = (List<Map<String, String>>) dataStream.get("indices");
        return indices.get(0).get("index_name");
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> getSettings(RestClient client, String indexName) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        return ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get("settings");
    }

    static void putMapping(RestClient client, String indexName) throws IOException {
        Request request = new Request("PUT", "/" + indexName + "/_mapping");
        request.setJsonEntity("""
            {
              "properties": {
                "numeric_field": {
                  "type": "integer"
                }
              }
            }
            """);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> getMappingProperties(RestClient client, String indexName) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_mapping");
        Map<String, Object> map = (Map<String, Object>) entityAsMap(client.performRequest(request)).get(indexName);
        Map<String, Object> mappings = (Map<String, Object>) map.get("mappings");
        return (Map<String, Object>) mappings.get("properties");
    }

    static void indexDoc(RestClient client, String dataStreamName, String doc) throws IOException {
        Request request = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        request.setJsonEntity(doc);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    static List<Object> searchDocs(RestClient client, String dataStreamName, String query) throws IOException {
        Request request = new Request("GET", "/" + dataStreamName + "/_search");
        request.setJsonEntity(query);
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client.performRequest(request)).get("hits");
        return (List<Object>) hits.get("hits");
    }

    @SuppressWarnings("unchecked")
    static Object getValueFromPath(Map<String, Object> map, List<String> path) {
        Map<String, Object> current = map;
        for (int i = 0; i < path.size(); i++) {
            Object value = current.get(path.get(i));
            if (i == path.size() - 1) {
                return value;
            }
            if (value == null) {
                throw new IllegalStateException("Path " + String.join(".", path) + " was not found in " + map);
            }
            if (value instanceof Map<?, ?> next) {
                current = (Map<String, Object>) next;
            } else {
                throw new IllegalStateException(
                    "Failed to reach the end of the path "
                        + String.join(".", path)
                        + " last reachable field was "
                        + path.get(i)
                        + " in "
                        + map
                );
            }
        }
        return current;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        if (super.restAdminSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restAdminSettings();
        } else {
            String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
            return Settings.builder().put(super.restAdminSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    @Before
    public void setup() throws Exception {
        client = client();
        AbstractDataStreamIT.waitForIndexTemplate(client, indexTemplateName());
    }

    protected abstract String indexTemplateName();

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
    }
}
