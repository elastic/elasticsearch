/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.indices.stats.FieldUsageShardResponse;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsRequest;
import org.elasticsearch.index.search.stats.FieldUsageStats;

import java.util.List;

public class StatelessFieldUsageStatsIT extends AbstractStatelessPluginIntegTestCase {

    public void testFieldUsageTrackingDisabledOnIndexNode() throws Exception {
        startMasterAndIndexNode();

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(5, 20);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource("field", "value").get();
        }

        // Pure indexing does not go through wrapSearcher, so stats must be empty before updates.
        FieldUsageStats statsBefore = getFieldUsageStats(indexName);
        assertFalse(statsBefore.hasField("_source"));

        // Updates trigger getForUpdate → wrapSearcher on the index node, which reads _source via
        // stored fields. If tracking were enabled _source would appear in the stats after these updates.
        int numUpdates = randomIntBetween(3, 10);
        for (int i = 0; i < numUpdates; i++) {
            client().prepareUpdate(indexName, Integer.toString(randomIntBetween(0, numDocs - 1))).setDoc("field", "updated-" + i).get();
        }

        // With tracking disabled on index nodes, stats must be unchanged: _source still absent.
        FieldUsageStats statsAfter = getFieldUsageStats(indexName);
        assertFalse(statsAfter.hasField("_source"));
    }

    private FieldUsageStats getFieldUsageStats(String indexName) throws Exception {
        List<FieldUsageShardResponse> shardStats = client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest(indexName))
            .get()
            .getStats()
            .get(indexName);
        return shardStats.stream().map(FieldUsageShardResponse::getStats).reduce(FieldUsageStats::add).get();
    }
}
