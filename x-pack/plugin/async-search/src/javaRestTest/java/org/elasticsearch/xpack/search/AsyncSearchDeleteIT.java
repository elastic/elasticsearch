/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.xpack.search.AsyncSearchSecurityIT.deleteAsyncSearch;
import static org.elasticsearch.xpack.search.AsyncSearchSecurityIT.extractResponseId;
import static org.elasticsearch.xpack.search.AsyncSearchSecurityIT.getAsyncStatus;
import static org.elasticsearch.xpack.search.AsyncSearchSecurityIT.submitAsyncSearchWithJsonBody;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AsyncSearchDeleteIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = getCluster();

    private static ElasticsearchCluster getCluster() {
        var builder = ElasticsearchCluster.local().module("x-pack-async-search").user("user1", "x-pack-test-password", "user1", false);
        if (Build.current().isSnapshot()) {
            // Only in non-release builds we can use the error_query
            builder = builder.module("test-error-query");
        }
        return builder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createIndex() throws IOException {
        client().performRequest(new Request("PUT", "/test_index"));
    }

    public void testDeleteWhileSearchIsRunning() throws IOException {
        assumeTrue("[error_query] is only available in snapshot builds", Build.current().isSnapshot());
        String user = "user1";
        String indexName = "test_index";
        String query = """
            {
              "query": {
                "error_query": {
                  "indices": [
                    {
                      "name": "*",
                      "error_type": "none",
                      "stall_time_seconds": 30
                    }
                  ]
                }
              }
            }""";

        Response submitResp = submitAsyncSearchWithJsonBody(indexName, query, TimeValue.timeValueMillis(10), user);
        assertOK(submitResp);
        String id = extractResponseId(submitResp);

        Response delResp = deleteAsyncSearch(id, user);
        assertOK(delResp);
        assertAcknowledged(delResp);

        ResponseException exc_deleted = expectThrows(ResponseException.class, () -> deleteAsyncSearch(id, user));
        assertThat(exc_deleted.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(exc_deleted.getMessage(), containsString("resource_not_found_exception"));

        ResponseException exc_status = expectThrows(ResponseException.class, () -> getAsyncStatus(id, user));
        assertThat(exc_status.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(exc_status.getMessage(), containsString("resource_not_found_exception"));
    }

}
