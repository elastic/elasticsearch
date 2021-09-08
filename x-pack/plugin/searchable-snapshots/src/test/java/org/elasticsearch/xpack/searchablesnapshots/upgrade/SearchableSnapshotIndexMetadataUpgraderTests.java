/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.stream.StreamSupport;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SearchableSnapshotIndexMetadataUpgraderTests extends ESTestCase {

    public void testNoUpgradeNeeded() {
        Metadata.Builder metadataBuilder = randomMetadata(
            normal(),
            full(),
            partial_8plusNoShardLimit(),
            shardLimitGroupFrozen(partial_7_13plus()),
            shardLimitGroupFrozen(partialNeedsUpgrade())
        );
        assertThat(needsUpgrade(metadataBuilder), is(false));
    }

    public void testNeedsUpgrade() {
        assertThat(
            needsUpgrade(
                addIndex(
                    partialNeedsUpgrade(),
                    randomMetadata(
                        normal(),
                        full(),
                        partial_7_13plus(),
                        partialNeedsUpgrade(),
                        shardLimitGroupFrozen(partialNeedsUpgrade())
                    )
                )
            ),
            is(true)
        );
    }

    public void testUpgradeIndices() {
        Metadata.Builder metadataBuilder = addIndex(
            partialNeedsUpgrade(),
            randomMetadata(normal(), full(), partial_7_13plus(), partialNeedsUpgrade(), shardLimitGroupFrozen(partialNeedsUpgrade()))
        );

        ClusterState originalState = clusterState(metadataBuilder);
        ClusterState upgradedState = SearchableSnapshotIndexMetadataUpgrader.upgradeIndices(originalState);

        assertThat(upgradedState, not(sameInstance(originalState)));
        assertThat(upgradedState.metadata().indices().size(), equalTo(originalState.metadata().indices().size()));

        assertTrue(StreamSupport.stream(upgradedState.metadata().spliterator(), false).anyMatch(upgraded -> {
            IndexMetadata original = originalState.metadata().index(upgraded.getIndex());
            assertThat(original, notNullValue());
            if (isPartial(upgraded) == false
                || ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.get(original.getSettings())
                    .equals(ShardLimitValidator.FROZEN_GROUP)) {
                assertThat(upgraded, sameInstance(original));
                return false;
            } else {
                assertThat(isPartial(upgraded), is(isPartial(original)));
                assertThat(upgraded.getNumberOfShards(), equalTo(original.getNumberOfShards()));
                assertThat(upgraded.getNumberOfReplicas(), equalTo(original.getNumberOfReplicas()));
                assertThat(
                    ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.get(upgraded.getSettings()),
                    equalTo(ShardLimitValidator.FROZEN_GROUP)
                );
                assertThat(upgraded.getSettingsVersion(), equalTo(original.getSettingsVersion() + 1));
                return true;
            }
        }));

        assertThat(SearchableSnapshotIndexMetadataUpgrader.needsUpgrade(upgradedState), is(false));
    }

    public void testNoopUpgrade() {
        Metadata.Builder metadataBuilder = randomMetadata(
            normal(),
            full(),
            partial_7_13plus(),
            shardLimitGroupFrozen(partialNeedsUpgrade()),
            partial_8plusNoShardLimit()
        );
        ClusterState originalState = clusterState(metadataBuilder);
        ClusterState upgradedState = SearchableSnapshotIndexMetadataUpgrader.upgradeIndices(originalState);
        assertThat(upgradedState, sameInstance(originalState));
    }

    private Settings normal() {
        return settings(VersionUtils.randomVersion(random())).build();
    }

    /**
     * Simulate an index mounted with no shard limit group. Notice that due to not applying the group during rolling upgrades, we can see
     * other than 7.12 versions here, but not 8.0 (since a rolling upgrade to 8.0 requires an upgrade to 7.latest first).
     */
    private Settings partialNeedsUpgrade() {
        return searchableSnapshotSettings(
            VersionUtils.randomVersionBetween(random(), Version.V_7_12_0, VersionUtils.getPreviousVersion(Version.V_8_0_0)),
            true
        );
    }

    /**
     * Simulate a 7.13plus mounted index with shard limit.
     */
    private Settings partial_7_13plus() {
        return shardLimitGroupFrozen(
            searchableSnapshotSettings(VersionUtils.randomVersionBetween(random(), Version.V_7_13_0, Version.CURRENT), true)
        );
    }

    /**
     * This is an illegal state, but we simulate it to capture that we do the version check
     */
    private Settings partial_8plusNoShardLimit() {
        return searchableSnapshotSettings(VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, Version.CURRENT), true);
    }

    private Settings full() {
        return searchableSnapshotSettings(VersionUtils.randomVersion(random()), false);
    }

    private Settings searchableSnapshotSettings(Version version, boolean partial) {
        Settings.Builder settings = settings(version);
        settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE);
        if (partial || randomBoolean()) {
            settings.put(SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING.getKey(), partial);
        }
        return settings.build();
    }

    private Settings shardLimitGroupFrozen(Settings settings) {
        return Settings.builder()
            .put(settings)
            .put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), ShardLimitValidator.FROZEN_GROUP)
            .build();
    }

    private Metadata.Builder addIndex(Settings settings, Metadata.Builder builder) {
        builder.put(
            IndexMetadata.builder(randomAlphaOfLength(10))
                .settings(settings)
                .numberOfShards(between(1, 10))
                .numberOfReplicas(between(0, 10))
                .build(),
            false
        );
        return builder;
    }

    private Metadata.Builder randomMetadata(Settings... indexSettingsList) {
        Metadata.Builder builder = new Metadata.Builder();
        for (Settings settings : indexSettingsList) {
            for (int i = 0; i < between(0, 10); ++i) {
                addIndex(settings, builder);
            }
        }
        return builder;
    }

    private boolean needsUpgrade(Metadata.Builder metadataBuilder) {
        return SearchableSnapshotIndexMetadataUpgrader.needsUpgrade(clusterState(metadataBuilder));
    }

    private ClusterState clusterState(Metadata.Builder metadataBuilder) {
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();
    }

    private boolean isPartial(IndexMetadata upgraded) {
        return SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(upgraded.getSettings());
    }
}
