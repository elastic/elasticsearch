/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.multi_node;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class GlobalCheckpointSyncActionIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        return getClientSettings("test-user", "x-pack-test-password");
    }

    @Override
    protected Settings restAdminSettings() {
        return getClientSettings("super-user", "x-pack-super-password");
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
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            client().performRequest("PUT", "test-index", Collections.emptyMap(), entity);
        }

        // wait for the replica to recover
        client().performRequest("GET", "/_cluster/health", Collections.singletonMap("wait_for_status", "green"));

        // index some documents
        final int numberOfDocuments = randomIntBetween(0, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.field("foo", i);
                }
                builder.endObject();
                final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
                client().performRequest("PUT", "/test-index/test-type/" + i, Collections.emptyMap(), entity);
            }
        }

        // we have to wait for the post-operation global checkpoint sync to propagate to the replica
        assertBusy(() -> {
            final Map<String, String> params = new HashMap<>(2);
            params.put("level", "shards");
            params.put("filter_path", "**.seq_no");
            final Response response = client().performRequest("GET", "/test-index/_stats", params);
            final ObjectPath path = ObjectPath.createFromResponse(response);
            // int looks funny here since global checkpoints are longs but the response parser does not know enough to treat them as long
            final int shard0GlobalCheckpoint = path.evaluate("indices.test-index.shards.0.0.seq_no.global_checkpoint");
            assertThat(shard0GlobalCheckpoint, equalTo(numberOfDocuments - 1));
            final int shard1GlobalCheckpoint = path.evaluate("indices.test-index.shards.0.1.seq_no.global_checkpoint");
            assertThat(shard1GlobalCheckpoint, equalTo(numberOfDocuments - 1));
        });
    }

}
