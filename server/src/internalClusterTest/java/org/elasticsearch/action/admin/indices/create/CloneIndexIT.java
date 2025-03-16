/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.elasticsearch.action.admin.indices.create.ShrinkIndexIT.assertNoResizeSourceIndexSettings;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CloneIndexIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCreateCloneIndex() {
        IndexVersion version = IndexVersionUtils.randomCompatibleWriteVersion(random());
        int numPrimaryShards = randomIntBetween(1, 5);
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("number_of_shards", numPrimaryShards).put("index.version.created", version)
        ).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
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
            assertHitCount(prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")), docs);

            if (createWithReplicas == false) {
                // bump replicas
                setReplicaCount(1, "target");
                ensureGreen();
                assertHitCount(prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")), docs);
            }

            for (int i = docs; i < 2 * docs; i++) {
                prepareIndex("target").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
            }
            flushAndRefresh();
            assertHitCount(prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")), 2 * docs);
            assertHitCount(prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")), docs);
            GetSettingsResponse target = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, "target").get();
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

    public void testResizeChangeIndexMode() {
        prepareCreate("source").setSettings(indexSettings(1, 0)).setMapping("@timestamp", "type=date", "host.name", "type=keyword").get();
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source");
        List<Settings> indexSettings = List.of(
            Settings.builder().put("index.mode", "logsdb").build(),
            Settings.builder().put("index.mode", "time_series").put("index.routing_path", "host.name").build(),
            Settings.builder().put("index.mode", "lookup").build()
        );
        for (Settings settings : indexSettings) {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
                indicesAdmin().prepareResizeIndex("source", "target").setResizeType(ResizeType.CLONE).setSettings(settings).get();
            });
            assertThat(error.getMessage(), equalTo("can't change setting [index.mode] during resize"));
        }
    }

    public void testResizeChangeSyntheticSource() {
        prepareCreate("source").setSettings(indexSettings(between(1, 5), 0))
            .setMapping("@timestamp", "type=date", "host.name", "type=keyword")
            .get();
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source");
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
            indicesAdmin().prepareResizeIndex("source", "target")
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder().put("index.mapping.source.mode", "synthetic").putNull("index.blocks.write").build())
                .get();
        });
        assertThat(error.getMessage(), containsString("can't change setting [index.mapping.source.mode] during resize"));
    }

    public void testResizeChangeRecoveryUseSyntheticSource() {
        prepareCreate("source").setSettings(
            indexSettings(between(1, 5), 0).put("index.mode", "logsdb")
                .put(
                    "index.version.created",
                    IndexVersionUtils.randomVersionBetween(
                        random(),
                        IndexVersions.USE_SYNTHETIC_SOURCE_FOR_RECOVERY,
                        IndexVersion.current()
                    )
                )
        ).setMapping("@timestamp", "type=date", "host.name", "type=keyword").get();
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source");
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
            indicesAdmin().prepareResizeIndex("source", "target")
                .setResizeType(ResizeType.CLONE)
                .setSettings(
                    Settings.builder()
                        .put(
                            "index.version.created",
                            IndexVersionUtils.randomVersionBetween(
                                random(),
                                IndexVersions.USE_SYNTHETIC_SOURCE_FOR_RECOVERY,
                                IndexVersion.current()
                            )
                        )
                        .put("index.recovery.use_synthetic_source", true)
                        .put("index.mode", "logsdb")
                        .putNull("index.blocks.write")
                        .build()
                )
                .get();
        });
        // The index.recovery.use_synthetic_source setting requires either index.mode or index.mapping.source.mode
        // to be present in the settings. Since these are all unmodifiable settings with a non-deterministic evaluation
        // order, any of them may trigger a failure first.
        assertThat(
            error.getMessage(),
            anyOf(
                containsString("can't change setting [index.mode] during resize"),
                containsString("can't change setting [index.recovery.use_synthetic_source] during resize")
            )
        );
    }

    public void testResizeChangeIndexSorts() {
        prepareCreate("source").setSettings(indexSettings(between(1, 5), 0))
            .setMapping("@timestamp", "type=date", "host.name", "type=keyword")
            .get();
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "source");
        ValidationException error = expectThrows(ValidationException.class, () -> {
            indicesAdmin().prepareResizeIndex("source", "target")
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder().putList("index.sort.field", List.of("@timestamp")).build())
                .get();
        });
        assertThat(error.getMessage(), containsString("can't override index sort when resizing an index"));
    }
}
