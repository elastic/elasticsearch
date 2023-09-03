/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;

public class RetentionLeaseStatsTests extends ESSingleNodeTestCase {

    public void testRetentionLeaseStats() throws InterruptedException {
        final Settings settings = indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        createIndex("index", settings);
        ensureGreen("index");
        final IndexShard primary = node().injector()
            .getInstance(IndicesService.class)
            .getShardOrNull(new ShardId(resolveIndex("index"), 0));
        final int length = randomIntBetween(0, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionTestUtils.assertNoFailureListener(r -> latch.countDown());
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
        }

        final IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("index").execute().actionGet();
        assertThat(indicesStats.getShards(), arrayWithSize(1));
        final RetentionLeaseStats retentionLeaseStats = indicesStats.getShards()[0].getRetentionLeaseStats();
        assertThat(
            RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(retentionLeaseStats.retentionLeases()),
            equalTo(currentRetentionLeases)
        );
    }

}
