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
            .setting("logger.org.elasticsearch.xpack.security.transport.CrossClusterAccessServerTransportFilter", "debug")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .module("transform")
            .module("reindex")
            .setting("logger.org.elasticsearch.xpack.security.authz.AuthorizationService", "debug")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                API_KEY_MAP_REF.compareAndSet(null, createCrossClusterAccessApiKey("""
                    [
                      {
                         "names": ["shared-transform-index"],
                         "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                      }
                    ]"""));
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
        configureRemoteClusters();

        // Fulfilling cluster
        {
            final Request createIndexRequest = new Request("PUT", "shared-transform-index");
            createIndexRequest.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "time": { "type": "date" },
                      "user": { "type": "keyword" },
                      "stars": { "type": "integer" },
                      "coolness": { "type": "integer" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest));

            // Index some documents, so we can attempt to transform them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 1, "date" : "2018-10-29T12:12:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 4, "date" : "2018-10-29T12:14:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 5, "date" : "2018-10-29T12:16:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 2, "date" : "2018-10-29T12:17:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 3, "date" : "2018-10-29T12:22:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 5, "date" : "2018-10-29T12:23:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "b", "stars": 1, "date" : "2018-10-29T12:32:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "a", "stars": 3, "date" : "2018-10-29T12:34:12.123456789Z"}
                {"index": {"_index": "shared-transform-index"}}
                {"user": "c", "stars": 4, "date" : "2018-10-29T12:35:12.123456789Z"}
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
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

            // Preview
            assertOK(performRequestWithRemoteTransformUser(new Request("GET", "/_transform/simple-remote-transform/_preview")));

            // Delete the transform
            assertOK(performRequestWithRemoteTransformUser(new Request("DELETE", "/_transform/simple-remote-transform")));
        }
    }

    private Response performRequestWithRemoteTransformUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_TRANSFORM_USER, PASS))
        );
        return client().performRequest(request);
    }
}
