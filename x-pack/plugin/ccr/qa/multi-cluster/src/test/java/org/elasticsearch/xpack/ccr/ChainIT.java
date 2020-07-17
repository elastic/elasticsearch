/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;

public class ChainIT extends ESCCRRestTestCase {

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "leader";
        final String middleIndexName = "middle";
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
        } else if ("middle".equals(targetCluster)) {
            logger.info("Running against middle cluster");
            followIndex("leader_cluster", leaderIndexName, middleIndexName);
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs, "filtered_field:true"));
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 3, "filtered_field:true"));
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "follow";
            followIndex("middle_cluster", middleIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3, "filtered_field:true"));

            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs + 3;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }

            try (RestClient middleClient = buildMiddleClient()) {
                assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 6, "filtered_field:true", middleClient));
            }

            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 6, "filtered_field:true"));
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
    }

}
