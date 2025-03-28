/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.esql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class EsqlPartialResultsIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("test-error-query")
        .module("x-pack-esql")
        .module("x-pack-ilm")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.query.allow_partial_results", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public Set<String> populateIndices() throws Exception {
        int nextId = 0;
        {
            createIndex("failing-index", Settings.EMPTY, """
                {
                    "runtime": {
                        "fail_me": {
                            "type": "long",
                            "script": {
                                "source": "",
                                "lang": "failing_field"
                            }
                        }
                    },
                    "properties": {
                        "v": {
                            "type": "long"
                        }
                    }
                }
                """);
            int numDocs = between(1, 50);
            for (int i = 0; i < numDocs; i++) {
                String id = Integer.toString(nextId++);
                Request doc = new Request("PUT", "failing-index/_doc/" + id);
                doc.setJsonEntity("{\"v\": " + id + "}");
                client().performRequest(doc);
            }

        }
        Set<String> okIds = new HashSet<>();
        {
            createIndex("ok-index", Settings.EMPTY, """
                {
                    "properties": {
                        "v": {
                            "type": "long"
                        }
                    }
                }
                """);
            int numDocs = between(1, 50);
            for (int i = 0; i < numDocs; i++) {
                String id = Integer.toString(nextId++);
                okIds.add(id);
                Request doc = new Request("PUT", "ok-index/_doc/" + id);
                doc.setJsonEntity("{\"v\": " + id + "}");
                client().performRequest(doc);
            }
        }
        refresh(client(), "failing-index,ok-index");
        return okIds;
    }

    @SuppressWarnings("unchecked")
    public void testPartialResult() throws Exception {
        Set<String> okIds = populateIndices();
        String query = """
            {
              "query": "FROM ok-index,failing-index | LIMIT 100 | KEEP fail_me,v"
            }
            """;
        // allow_partial_results = true
        {
            Request request = new Request("POST", "/_query");
            request.setJsonEntity(query);
            if (randomBoolean()) {
                request.addParameter("allow_partial_results", "true");
            }
            Response resp = client().performRequest(request);
            Map<String, Object> results = entityAsMap(resp);
            logger.info("--> results {}", results);
            assertThat(results.get("is_partial"), equalTo(true));
            List<?> columns = (List<?>) results.get("columns");
            assertThat(columns, equalTo(List.of(Map.of("name", "fail_me", "type", "long"), Map.of("name", "v", "type", "long"))));
            List<?> values = (List<?>) results.get("values");
            assertThat(values.size(), lessThanOrEqualTo(okIds.size()));
            Map<String, Object> localInfo = (Map<String, Object>) XContentMapValues.extractValue(
                results,
                "_clusters",
                "details",
                "(local)"
            );
            assertNotNull(localInfo);
            assertThat(XContentMapValues.extractValue(localInfo, "_shards", "successful"), equalTo(0));
            assertThat(
                XContentMapValues.extractValue(localInfo, "_shards", "failed"),
                equalTo(XContentMapValues.extractValue(localInfo, "_shards", "total"))
            );
            List<Map<String, Object>> failures = (List<Map<String, Object>>) XContentMapValues.extractValue(localInfo, "failures");
            assertThat(failures, hasSize(1));
            assertThat(
                failures.get(0).get("reason"),
                equalTo(Map.of("type", "illegal_state_exception", "reason", "Accessing failing field"))
            );
        }
        // allow_partial_results = false
        {
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("""
                {
                  "query": "FROM ok-index,failing-index | LIMIT 100"
                }
                """);
            request.addParameter("allow_partial_results", "false");
            var error = expectThrows(ResponseException.class, () -> client().performRequest(request));
            Response resp = error.getResponse();
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(500));
            assertThat(EntityUtils.toString(resp.getEntity()), containsString("Accessing failing field"));
        }

    }

    @SuppressWarnings("unchecked")
    public void testFailureFromRemote() throws Exception {
        setupRemoteClusters();
        try {
            Set<String> okIds = populateIndices();
            String query = """
                {
                  "query": "FROM *:ok-index,*:failing-index | LIMIT 100 | KEEP fail_me,v"
                }
                """;
            // allow_partial_results = true
            Request request = new Request("POST", "/_query");
            request.setJsonEntity(query);
            if (randomBoolean()) {
                request.addParameter("allow_partial_results", "true");
            }
            Response resp = client().performRequest(request);
            Map<String, Object> results = entityAsMap(resp);
            logger.info("--> results {}", results);
            assertThat(results.get("is_partial"), equalTo(true));
            List<?> columns = (List<?>) results.get("columns");
            assertThat(columns, equalTo(List.of(Map.of("name", "fail_me", "type", "long"), Map.of("name", "v", "type", "long"))));
            List<?> values = (List<?>) results.get("values");
            assertThat(values.size(), lessThanOrEqualTo(okIds.size()));
            Map<String, Object> remoteCluster = (Map<String, Object>) XContentMapValues.extractValue(
                results,
                "_clusters",
                "details",
                "cluster_one"
            );
            assertNotNull(remoteCluster);
            assertThat(XContentMapValues.extractValue(remoteCluster, "_shards", "successful"), equalTo(0));
            assertThat(
                XContentMapValues.extractValue(remoteCluster, "_shards", "failed"),
                equalTo(XContentMapValues.extractValue(remoteCluster, "_shards", "total"))
            );
            List<Map<String, Object>> failures = (List<Map<String, Object>>) XContentMapValues.extractValue(remoteCluster, "failures");
            assertThat(failures, hasSize(1));
            assertThat(
                failures.get(0).get("reason"),
                equalTo(Map.of("type", "illegal_state_exception", "reason", "Accessing failing field"))
            );
        } finally {
            removeRemoteCluster();
        }
    }

    private void setupRemoteClusters() throws IOException {
        String settings = String.format(Locale.ROOT, """
            {
                "persistent": {
                    "cluster": {
                        "remote": {
                            "cluster_one": {
                                "seeds": [ "%s" ],
                                "skip_unavailable": false
                            }
                        }
                    }
                }
            }
            """, cluster.getTransportEndpoints());
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(settings);
        client().performRequest(request);
    }

    private void removeRemoteCluster() throws IOException {
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity("""
            {"persistent": { "cluster.*": null}}
            """);
        client().performRequest(settingsRequest);
    }
}
