/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.multi_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.ClassRule;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class GlobalCheckpointSyncActionIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-user", "x-pack-test-password", "test")
        .user("super-user", "x-pack-super-password")
        .build();

    @Override
    protected Settings restClientSettings() {
        return getClientSettings("test-user", "x-pack-test-password");
    }

    @Override
    protected Settings restAdminSettings() {
        return getClientSettings("super-user", "x-pack-super-password");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private Settings getClientSettings(final String username, final String password) {
        final String token = basicAuthHeaderValue(username, new SecureString(password.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /*
     * The post-operation global checkpoint sync runs privileged as the system user otherwise the sync would be denied to restricted users.
     * This test ensures that these post-operation syncs are successful otherwise the global checkpoint would not have advanced on the
     * replica.
     */
    public void testGlobalCheckpointSyncActionRunsAsPrivilegedUser() throws Exception {
        // create the test-index index
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", 1);
                    builder.field("index.number_of_replicas", 1);
                }
                builder.endObject();
            }
            builder.endObject();
            Request createIndexRequest = new Request("PUT", "/test-index");
            createIndexRequest.setJsonEntity(Strings.toString(builder));
            client().performRequest(createIndexRequest);
        }

        // wait for the replica to recover
        Request healthRequest = new Request("GET", "/_cluster/health");
        healthRequest.addParameter("wait_for_status", "green");
        client().performRequest(healthRequest);

        // index some documents
        final int numberOfDocuments = randomIntBetween(0, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.field("foo", i);
                }
                builder.endObject();
                Request indexRequest = new Request("PUT", "/test-index/_doc/" + i);
                indexRequest.setJsonEntity(Strings.toString(builder));
                client().performRequest(indexRequest);
            }
        }

        // we have to wait for the post-operation global checkpoint sync to propagate to the replica
        assertBusy(() -> {
            final Request request = new Request("GET", "/test-index/_stats");
            request.addParameter("level", "shards");
            request.addParameter("filter_path", "**.seq_no");
            final Response response = client().performRequest(request);
            final ObjectPath path = ObjectPath.createFromResponse(response);
            // int looks funny here since global checkpoints are longs but the response parser does not know enough to treat them as long
            final int shard0GlobalCheckpoint = path.evaluate("indices.test-index.shards.0.0.seq_no.global_checkpoint");
            assertThat(shard0GlobalCheckpoint, equalTo(numberOfDocuments - 1));
            final int shard1GlobalCheckpoint = path.evaluate("indices.test-index.shards.0.1.seq_no.global_checkpoint");
            assertThat(shard1GlobalCheckpoint, equalTo(numberOfDocuments - 1));
        });
    }

}
