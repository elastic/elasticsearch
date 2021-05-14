/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class XPackUsageIT extends ESCCRRestTestCase {

    public void testXPackCcrUsage() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }

        Map<?, ?> previousUsage = getCcrUsage();
        putAutoFollowPattern("my_pattern", "leader_cluster", "messages-*");

        // This index should be auto followed:
        createLeaderIndex("messages-20200101");
        // This index will be followed manually
        createLeaderIndex("my_index");
        followIndex("my_index", "my_index");

        int previousFollowerIndicesCount = (Integer) previousUsage.get("follower_indices_count");
        int previousAutoFollowPatternsCount = (Integer) previousUsage.get("auto_follow_patterns_count");
        assertBusy(() -> {
            Map<?, ?> ccrUsage = getCcrUsage();
            assertThat(ccrUsage.get("follower_indices_count"), equalTo(previousFollowerIndicesCount + 2));
            assertThat(ccrUsage.get("auto_follow_patterns_count"), equalTo(previousAutoFollowPatternsCount + 1));
            assertThat((Integer) ccrUsage.get("last_follow_time_in_millis"), greaterThanOrEqualTo(0));
            // We need to wait until index following is active for auto followed indices:
            // (otherwise pause follow may fail, if there are no shard follow tasks, in case this test gets executed too quickly)
            assertIndexFollowingActive("messages-20200101");
        });

        deleteAutoFollowPattern("my_pattern");
        pauseFollow("messages-20200101");
        closeIndex("messages-20200101");
        unfollow("messages-20200101");

        pauseFollow("my_index");
        closeIndex("my_index");
        unfollow("my_index");

        assertBusy(() -> {
            Map<?, ?> ccrUsage = getCcrUsage();
            assertThat(ccrUsage.get("follower_indices_count"), equalTo(previousFollowerIndicesCount));
            assertThat(ccrUsage.get("auto_follow_patterns_count"), equalTo(previousAutoFollowPatternsCount));
            if (previousFollowerIndicesCount == 0) {
                assertThat(ccrUsage.get("last_follow_time_in_millis"), nullValue());
            } else {
                assertThat((Integer) ccrUsage.get("last_follow_time_in_millis"), greaterThanOrEqualTo(0));
            }
        });
    }

    private void createLeaderIndex(String indexName) throws IOException {
        try (RestClient leaderClient = buildLeaderClient()) {
            Request request = new Request("PUT", "/" + indexName);
            request.setJsonEntity("{}");
            assertOK(leaderClient.performRequest(request));
        }
    }

    private Map<?, ?> getCcrUsage() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        Map<String, ?> response = toMap(client().performRequest(request));
        logger.info("xpack usage response={}", response);
        return  (Map<?, ?>) response.get("ccr");
    }

    private void assertIndexFollowingActive(String expectedFollowerIndex) throws IOException {
        Request statsRequest = new Request("GET", "/" + expectedFollowerIndex + "/_ccr/info");
        Map<?, ?> response = toMap(client().performRequest(statsRequest));
        String actualFollowerIndex = ObjectPath.eval("follower_indices.0.follower_index", response);
        assertThat(actualFollowerIndex, equalTo(expectedFollowerIndex));
        String followStatus = ObjectPath.eval("follower_indices.0.status", response);
        assertThat(followStatus, equalTo("active"));
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

}
