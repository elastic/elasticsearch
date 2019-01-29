/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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
            assertThat((Integer) ccrUsage.get("time_since_last_index_followed"), greaterThanOrEqualTo(0));
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
            assertThat((Integer) ccrUsage.get("time_since_last_index_followed"), greaterThanOrEqualTo(0));
        });
    }

    private void createLeaderIndex(String indexName) throws IOException {
        try (RestClient leaderClient = buildLeaderClient()) {
            Settings settings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .build();
            Request request = new Request("PUT", "/" + indexName);
            request.setJsonEntity("{\"settings\": " + Strings.toString(settings) + "}");
            assertOK(leaderClient.performRequest(request));
        }
    }

    private Map<?, ?> getCcrUsage() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        Map<String, ?> response = toMap(client().performRequest(request));
        logger.info("xpack usage response={}", response);
        return  (Map<?, ?>) response.get("ccr");
    }

}
