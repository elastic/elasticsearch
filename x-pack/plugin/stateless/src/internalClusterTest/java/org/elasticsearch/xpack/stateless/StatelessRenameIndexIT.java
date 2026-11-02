/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.indices.rename.RenameIndexAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;

import static org.hamcrest.Matchers.equalTo;

/**
 * Renames an index in a stateless cluster (one index node, one search node) and checks that writes, refreshes and searches keep
 * working through the index and search tiers afterwards.
 */
@TestLogging(
    value = "org.elasticsearch.action.bulk:DEBUG,org.elasticsearch.indices.cluster:DEBUG,org.elasticsearch.cluster.service:DEBUG,"
        + "org.elasticsearch.cluster.coordination:DEBUG,org.elasticsearch.xpack.stateless:DEBUG",
    reason = "diagnose hangs after renaming an index in a stateless cluster"
)
public class StatelessRenameIndexIT extends AbstractStatelessPluginIntegTestCase {

    public void testWriteAndSearchAfterRename() {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String original = "stateless-rename-original";
        final String renamed = "stateless-rename-renamed";
        // opt out of the test framework rename so that the rename below is the only one
        createIndex(original, indexSettings(1, 1).build(), false);
        ensureGreen(original);
        client().prepareIndex(original)
            .setSource("field", "before")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TimeValue.timeValueSeconds(30));
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(original), 1L);

        var response = client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, original, renamed)
        ).actionGet(TimeValue.timeValueSeconds(30));
        assertTrue(response.isAcknowledged());
        ensureGreen(renamed);

        // a write with a dynamic mapping update, then a write without one
        client().prepareIndex(renamed).setSource("newfield", "after").get(TimeValue.timeValueSeconds(30));
        client().prepareIndex(renamed).setSource("field", "after").get(TimeValue.timeValueSeconds(30));
        assertThat(indicesAdmin().prepareRefresh(renamed).get(TimeValue.timeValueSeconds(30)).getFailedShards(), equalTo(0));
        ElasticsearchAssertions.assertHitCount(client().prepareSearch(renamed), 3L);
    }
}
