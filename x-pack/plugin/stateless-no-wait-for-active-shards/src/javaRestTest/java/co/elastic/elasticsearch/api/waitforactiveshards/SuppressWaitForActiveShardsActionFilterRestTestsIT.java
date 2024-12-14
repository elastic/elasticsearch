/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.api.waitforactiveshards;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Rule;

import java.io.IOException;

/**
 * Tests to verify that {@link SuppressWaitForActiveShardsActionFilter} works.
 */
public class SuppressWaitForActiveShardsActionFilterRestTestsIT extends ESRestTestCase {

    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static Request addWaitForActiveShardsParam(Request request, String activeShardCount) {
        if (activeShardCount != null) {
            request.addParameter("wait_for_active_shards", activeShardCount);
        }
        return request;
    }

    private static String randomActiveShardCount() {
        return randomFrom("all", Integer.toString(between(0, 5)), null);
    }

    public void testIndexingIgnoresWaitForActiveShards() throws IOException {
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        cluster.stopNode(1, true);

        final var bulkRequest = new Request(HttpMethod.POST.name(), '/' + indexName + "/_bulk");
        bulkRequest.setJsonEntity("""
            {"index":{"_id":"bulk_test_doc"}}
            {"foo":"bar"}
            {"update":{"_id":"bulk_test_doc"}}
            {"doc":{"foo":"baz"}}
            {"delete":{"_id":"bulk_test_doc"}}
            """);
        assertOK(client().performRequest(addWaitForActiveShardsParam(bulkRequest, randomActiveShardCount())));

        assertOK(
            client().performRequest(
                addWaitForActiveShardsParam(
                    newXContentRequest(
                        HttpMethod.PUT,
                        '/' + indexName + randomFrom("/_create/test_doc", "/_doc/test_doc"),
                        (builder, params) -> builder.field("foo", "bar")
                    ),
                    randomActiveShardCount()
                )
            )
        );
        assertOK(
            client().performRequest(
                addWaitForActiveShardsParam(
                    newXContentRequest(
                        HttpMethod.POST,
                        '/' + indexName + "/_update/test_doc",
                        (builder, params) -> builder.startObject("doc").field("foo", "baz").endObject()
                    ),
                    randomActiveShardCount()
                )
            )
        );
        assertOK(
            client().performRequest(
                addWaitForActiveShardsParam(
                    new Request(HttpMethod.DELETE.name(), '/' + indexName + "/_doc/test_doc"),
                    randomActiveShardCount()
                )
            )
        );
    }
}
