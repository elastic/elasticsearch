/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ReindexRemoteIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("reindex")
        .module("reindex-management")
        .module("rest-root")
        .setting("reindex.remote.whitelist", "127.0.0.1:*")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    private String getRemoteHost() throws Exception {
        Map<String, Object> nodesInfo = entityAsMap(client().performRequest(new Request("GET", "/_nodes/http")));
        Map<String, Object> nodes = (Map<String, Object>) nodesInfo.get("nodes");
        Map<String, Object> nodeInfo = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> http = (Map<String, Object>) nodeInfo.get("http");
        return "http://" + http.get("publish_address");
    }

    public void testGetReindexDescriptionStripsRemoteInfoSensitiveFields() throws Exception {
        assumeTrue(
            "reindex resilience endpoints available",
            clusterHasCapability("GET", "/_reindex/{task_id}", List.of(), List.of("reindex_management_api")).orElse(false)
        );
        Request indexRequest = new Request("POST", "/remote_src/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity("{\"field\": \"value\"}");
        client().performRequest(indexRequest);

        String remoteHost = getRemoteHost();

        Request reindexRequest = new Request("POST", "/_reindex");
        reindexRequest.addParameter("wait_for_completion", "false");
        reindexRequest.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "source": {
                "remote": {
                  "host": "%s",
                  "username": "testuser",
                  "password": "testpass"
                },
                "index": "remote_src",
                "query": {
                  "match_all": {}
                }
              },
              "dest": {
                "index": "dest"
              },
              "script": {
                "source": "ctx._source.tag = 'host=localhost port=9200 username=admin password=secret'"
              }
            }""", remoteHost));

        Response reindexResponse = client().performRequest(reindexRequest);
        String taskId = (String) entityAsMap(reindexResponse).get("task");
        assertNotNull("reindex did not return a task id", taskId);

        Request getReindexRequest = new Request("GET", "/_reindex/" + taskId);
        getReindexRequest.addParameter("wait_for_completion", "true");
        Response getResponse = client().performRequest(getReindexRequest);
        Map<String, Object> body = entityAsMap(getResponse);

        assertThat(body.get("completed"), is(true));
        URI remoteUri = URI.create(remoteHost);
        String expectedDescription = "reindex from [host="
            + remoteUri.getHost()
            + " port="
            + remoteUri.getPort()
            + "][remote_src] to [dest]";
        assertThat(body.get("description"), equalTo(expectedDescription));
    }

    public void testTaskDescriptionExcludesSensitiveFields() throws Exception {
        Request indexRequest = new Request("POST", "/remote_src/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity("{\"field\": \"value\"}");
        client().performRequest(indexRequest);

        String remoteHost = getRemoteHost();

        Request reindexRequest = new Request("POST", "/_reindex");
        reindexRequest.addParameter("wait_for_completion", "false");
        reindexRequest.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "source": {
                "remote": {
                  "host": "%s",
                  "username": "testuser",
                  "password": "testpass"
                },
                "index": "remote_src",
                "query": {
                  "match_all": {}
                }
              },
              "dest": {
                "index": "dest"
              }
            }""", remoteHost));

        Response reindexResponse = client().performRequest(reindexRequest);
        String taskId = (String) entityAsMap(reindexResponse).get("task");
        assertNotNull("reindex did not return a task id", taskId);

        Request getTaskRequest = new Request("GET", "/_tasks/" + taskId);
        getTaskRequest.addParameter("wait_for_completion", "true");
        getTaskRequest.addParameter("timeout", "30s");
        Response taskResponse = client().performRequest(getTaskRequest);
        Map<String, Object> body = entityAsMap(taskResponse);

        @SuppressWarnings("unchecked")
        Map<String, Object> task = (Map<String, Object>) body.get("task");
        String description = (String) task.get("description");

        URI remoteUri = URI.create(remoteHost);
        String expectedDescription = "reindex from [host="
            + remoteUri.getHost()
            + " port="
            + remoteUri.getPort()
            + "][remote_src] to [dest]";
        assertThat(description, equalTo(expectedDescription));
    }
}
