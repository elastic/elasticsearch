/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.action.admin.indices.create.ShrinkIndexIT.assertNoResizeSourceIndexSettings;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class CloneIndexIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCreateCloneIndex() {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        int numPrimaryShards = randomIntBetween(1, 5);
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("number_of_shards", numPrimaryShards).put("index.version.created", version)
        ).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        internalCluster().ensureAtLeastNumDataNodes(2);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source");
        ensureGreen();

        final IndicesStatsResponse sourceStats = indicesAdmin().prepareStats("source").setSegments(true).get();

        // disable rebalancing to be able to capture the right stats. balancing can move the target primary
        // making it hard to pin point the source shards.
        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none"));
        try {

            final boolean createWithReplicas = randomBoolean();
            assertAcked(
                indicesAdmin().prepareResizeIndex("source", "target")
                    .setResizeType(ResizeType.CLONE)
                    .setSettings(
                        Settings.builder().put("index.number_of_replicas", createWithReplicas ? 1 : 0).putNull("index.blocks.write").build()
                    )
                    .get()
            );
            ensureGreen();
            assertNoResizeSourceIndexSettings("target");

            final IndicesStatsResponse targetStats = indicesAdmin().prepareStats("target").get();
            assertThat(targetStats.getIndex("target").getIndexShards().keySet().size(), equalTo(numPrimaryShards));

            for (int i = 0; i < numPrimaryShards; i++) {
                final SeqNoStats sourceSeqNoStats = sourceStats.getIndex("source").getIndexShards().get(i).getAt(0).getSeqNoStats();
                final SeqNoStats targetSeqNoStats = targetStats.getIndex("target").getIndexShards().get(i).getAt(0).getSeqNoStats();
                assertEquals(sourceSeqNoStats.getMaxSeqNo(), targetSeqNoStats.getMaxSeqNo());
                assertEquals(targetSeqNoStats.getMaxSeqNo(), targetSeqNoStats.getLocalCheckpoint());
            }

            final int size = docs > 0 ? 2 * docs : 1;
            assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);

            if (createWithReplicas == false) {
                // bump replicas
                setReplicaCount(1, "target");
                ensureGreen();
                assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
            }

            for (int i = docs; i < 2 * docs; i++) {
                client().prepareIndex("target").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
            }
            flushAndRefresh();
            assertHitCount(
                client().prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                2 * docs
            );
            assertHitCount(client().prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
            GetSettingsResponse target = indicesAdmin().prepareGetSettings("target").get();
            assertThat(
                target.getIndexToSettings().get("target").getAsVersionId("index.version.created", IndexVersion::fromId),
                equalTo(version)
            );
        } finally {
            // clean up
            updateClusterSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)
            );
        }

    }

}
