/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2, numClientNodes = 0)
public class AutoExpandReplicasIT extends ESIntegTestCase {

    /**
     * Verifies that when no data node is available to host a replica, number of replicas are contracted to 0.
     */
    public void testClampToMinMax() throws Exception {
        final String indexName = "myindex";
        final String autoExpandValue = randomFrom("0-1", "0-all");
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, autoExpandValue)
                .build()
        );

        final int initialReplicas = autoExpandValue.equals("0-1") ? 1 : internalCluster().numDataNodes() - 1;

        assertBusy(() -> {
            assertThat(
                indicesAdmin().prepareGetSettings(indexName)
                    .setNames("index.number_of_replicas")
                    .get()
                    .getSetting(indexName, "index.number_of_replicas"),
                equalTo(String.valueOf(initialReplicas))
            );
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._id", "non-existing-node"), indexName);

        assertBusy(() -> {
            assertThat(
                indicesAdmin().prepareGetSettings(indexName)
                    .setNames("index.number_of_replicas")
                    .get()
                    .getSetting(indexName, "index.number_of_replicas"),
                equalTo("0")
            );
        });

        // Remove the setting
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._id", ""), indexName);

        assertBusy(() -> {
            assertThat(
                indicesAdmin().prepareGetSettings(indexName)
                    .setNames("index.number_of_replicas")
                    .get()
                    .getSetting(indexName, "index.number_of_replicas"),
                equalTo(String.valueOf(initialReplicas))
            );
        });
    }
}
