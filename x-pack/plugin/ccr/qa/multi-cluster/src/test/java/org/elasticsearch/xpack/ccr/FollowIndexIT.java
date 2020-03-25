/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FollowIndexIT extends ESCCRRestTestCase {

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            String mapping = "";
            if (randomBoolean()) { // randomly do source filtering on indexing
                mapping =
                    "\"_source\": {" +
                    "  \"includes\": [\"field\"]," +
                    "  \"excludes\": [\"filtered_field\"]" +
                    "}";
            }
            createIndex(leaderIndexName, Settings.EMPTY, mapping);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
            }
            refresh(leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs, "filtered_field:true");
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "test_index2";
            followIndex(leaderIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs, "filtered_field:true"));
            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            resumeFollow(followIndexName);
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));
            assertBusy(() -> verifyCcrMonitoring(leaderIndexName, followIndexName), 30, TimeUnit.SECONDS);

            pauseFollow(followIndexName);
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_close")));
            assertOK(client().performRequest(new Request("POST", "/" + followIndexName + "/_ccr/unfollow")));
            Exception e = expectThrows(ResponseException.class, () -> resumeFollow(followIndexName));
            assertThat(e.getMessage(), containsString("follow index [" + followIndexName + "] does not have ccr metadata"));
        }
    }

    public void testFollowNonExistingLeaderIndex() throws Exception {
        if ("follow".equals(targetCluster) == false) {
            logger.info("skipping test, waiting for target cluster [follow]" );
            return;
        }
        ResponseException e = expectThrows(ResponseException.class, () -> resumeFollow("non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        e = expectThrows(ResponseException.class, () -> followIndex("non-existing-index", "non-existing-index"));
        assertThat(e.getMessage(), containsString("no such index [non-existing-index]"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

}
