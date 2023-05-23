/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RemoteClusterSecurityTransformIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .module("transform")
            .module("reindex")
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .module("transform")
            .module("reindex")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                API_KEY_MAP_REF.compareAndSet(null, createCrossClusterAccessApiKey("""
                    {
                        "search": [
                          {
                              "names": ["shared-transform-index"]
                          }
                        ]
                    }"""));
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_TRANSFORM_USER, PASS.toString(), "transform_admin,transform_remote_shared_index")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterTransform() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster
        {
            final Request createIndexRequest1 = new Request("PUT", "shared-transform-index");
            createIndexRequest1.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "user": { "type": "keyword" },
                      "stars": { "type": "integer" },
                      "coolness": { "type": "integer" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest1));

            // Index some documents, so we can attempt to transform them from the querying cluster
            final Request bulkRequest1 = new Request("POST", "/_bulk?refresh=true");
            bulkRequest1.setJsonEntity(Strings.format("""
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 1}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 4}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 5}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 2}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 3}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 5}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 1}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 3}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "c", "stars": 4}
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest1));

            // Create another index that the transform user does not have access
            final Request createIndexRequest2 = new Request("PUT", "private-transform-index");
            createIndexRequest2.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "user": { "type": "keyword" },
                      "stars": { "type": "integer" },
                      "coolness": { "type": "integer" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest2));
        }

        // Query cluster
        {
            // Create a transform
            final var putTransformRequest = new Request("PUT", "/_transform/simple-remote-transform");
            putTransformRequest.setJsonEntity("""
                {
                  "source": { "index": "my_remote_cluster:shared-transform-index" },
                  "dest": { "index": "simple-remote-transform" },
                  "pivot": {
                    "group_by": { "user": {"terms": {"field": "user"}}},
                    "aggs": {"avg_stars": {"avg": {"field": "stars"}}}
                  }
                }
                """);
            assertOK(performRequestWithRemoteTransformUser(putTransformRequest));
            final ObjectPath getTransformObjPath = assertOKAndCreateObjectPath(
                performRequestWithRemoteTransformUser(new Request("GET", "/_transform/simple-remote-transform"))
            );
            assertThat(getTransformObjPath.evaluate("count"), equalTo(1));
            assertThat(getTransformObjPath.evaluate("transforms.0.id"), equalTo("simple-remote-transform"));

            // Start the transform
            assertOK(performRequestWithRemoteTransformUser(new Request("POST", "/_transform/simple-remote-transform/_start")));

            // Get the stats
            final Request transformStatsRequest = new Request("GET", "/_transform/simple-remote-transform/_stats");
            final ObjectPath transformStatsObjPath = assertOKAndCreateObjectPath(
                performRequestWithRemoteTransformUser(transformStatsRequest)
            );
            assertThat(transformStatsObjPath.evaluate("node_failures"), nullValue());
            assertThat(transformStatsObjPath.evaluate("count"), equalTo(1));
            assertThat(transformStatsObjPath.evaluate("transforms.0.id"), equalTo("simple-remote-transform"));

            // Stop the transform and force it to complete
            assertOK(
                performRequestWithRemoteTransformUser(
                    new Request("POST", "/_transform/simple-remote-transform/_stop?wait_for_completion=true&wait_for_checkpoint=true")
                )
            );

            // Get stats again
            final ObjectPath transformStatsObjPath2 = assertOKAndCreateObjectPath(
                performRequestWithRemoteTransformUser(transformStatsRequest)
            );
            assertThat(transformStatsObjPath2.evaluate("node_failures"), nullValue());
            assertThat(transformStatsObjPath2.evaluate("count"), equalTo(1));
            assertThat(transformStatsObjPath2.evaluate("transforms.0.state"), equalTo("stopped"));
            assertThat(transformStatsObjPath2.evaluate("transforms.0.checkpointing.last.checkpoint"), equalTo(1));

            // Ensure transformed data is available locally
            final ObjectPath searchObjPath = assertOKAndCreateObjectPath(
                performRequestWithRemoteTransformUser(
                    new Request("GET", "/simple-remote-transform/_search?sort=user&rest_total_hits_as_int=true")
                )
            );
            assertThat(searchObjPath.evaluate("hits.total"), equalTo(3));
            assertThat(searchObjPath.evaluate("hits.hits.0._index"), equalTo("simple-remote-transform"));
            assertThat(searchObjPath.evaluate("hits.hits.0._source.avg_stars"), equalTo(3.6));
            assertThat(searchObjPath.evaluate("hits.hits.0._source.user"), equalTo("a"));
            assertThat(searchObjPath.evaluate("hits.hits.1._source.avg_stars"), equalTo(2.0));
            assertThat(searchObjPath.evaluate("hits.hits.1._source.user"), equalTo("b"));
            assertThat(searchObjPath.evaluate("hits.hits.2._source.avg_stars"), equalTo(4.0));
            assertThat(searchObjPath.evaluate("hits.hits.2._source.user"), equalTo("c"));

            // Preview
            assertOK(performRequestWithRemoteTransformUser(new Request("GET", "/_transform/simple-remote-transform/_preview")));

            // Delete the transform
            assertOK(performRequestWithRemoteTransformUser(new Request("DELETE", "/_transform/simple-remote-transform")));

            // Create a transform targeting an index without permission
            final var putTransformRequest2 = new Request("PUT", "/_transform/invalid");
            putTransformRequest2.setJsonEntity("""
                {
                  "source": { "index": "my_remote_cluster:private-transform-index" },
                  "dest": { "index": "simple-remote-transform" },
                  "pivot": {
                    "group_by": { "user": {"terms": {"field": "user"}}},
                    "aggs": {"avg_stars": {"avg": {"field": "stars"}}}
                  }
                }
                """);
            assertOK(performRequestWithRemoteTransformUser(putTransformRequest2));
            // It errors when trying to preview it
            final ResponseException e = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteTransformUser(new Request("GET", "/_transform/invalid/_preview"))
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Source indices have been deleted or closed"));
        }
    }

    private Response performRequestWithRemoteTransformUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_TRANSFORM_USER, PASS))
        );
        return client().performRequest(request);
    }
}
