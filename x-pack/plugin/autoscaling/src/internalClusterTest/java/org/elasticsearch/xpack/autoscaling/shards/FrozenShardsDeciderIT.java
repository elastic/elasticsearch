/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class FrozenShardsDeciderIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateAutoscalingAndSearchableSnapshots.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        if (DiscoveryNode.canContainData(otherSettings)) {
            builder.put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(10, ByteSizeUnit.MB));
        }
        return builder.build();
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testScale() throws Exception {
        final String indexName = "index";
        final String restoredIndexName = "restored";
        final String fsRepoName = randomAlphaOfLength(10);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs");
        putAutoscalingPolicy("frozen");
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));

        indexRandom(
            randomBoolean(),
            IntStream.range(0, 10).mapToObj(i -> client().prepareIndex(indexName).setSource()).collect(Collectors.toList())
        );

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);

        assertThat(capacity().results().get("frozen").requiredCapacity().total().memory(), equalTo(ByteSizeValue.ZERO));

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );
        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        assertThat(
            capacity().results().get("frozen").requiredCapacity().total().memory(),
            equalTo(FrozenShardsDeciderService.DEFAULT_MEMORY_PER_SHARD)
        );
    }

    private GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        return client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
    }

    private void putAutoscalingPolicy(String policyName) {
        // randomly set the setting to verify it can be set.
        Settings settings = randomBoolean()
            ? Settings.EMPTY
            : Settings.builder()
                .put(FrozenShardsDeciderService.MEMORY_PER_SHARD.getKey(), FrozenShardsDeciderService.DEFAULT_MEMORY_PER_SHARD)
                .build();
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of(DataTier.DATA_FROZEN)),
            new TreeMap<>(Map.of(FrozenShardsDeciderService.NAME, settings))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

}
