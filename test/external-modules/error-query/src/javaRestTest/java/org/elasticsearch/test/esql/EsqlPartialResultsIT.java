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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class EsqlPartialResultsIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("test-error-query")
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
            assertThat(results.get("is_partial"), equalTo(true));
            List<?> columns = (List<?>) results.get("columns");
            assertThat(columns, equalTo(List.of(Map.of("name", "fail_me", "type", "long"), Map.of("name", "v", "type", "long"))));
            List<?> values = (List<?>) results.get("values");
            assertThat(values.size(), lessThanOrEqualTo(okIds.size()));
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
}
