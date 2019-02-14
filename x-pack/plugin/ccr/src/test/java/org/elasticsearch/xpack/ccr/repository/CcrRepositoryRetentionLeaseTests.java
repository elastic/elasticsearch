/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CcrRepositoryRetentionLeaseTests extends ESTestCase {

    public void test() {
        final RepositoryMetaData repositoryMetaData = mock(RepositoryMetaData.class);
        when(repositoryMetaData.name()).thenReturn(CcrRepository.NAME_PREFIX);
        final Set<Setting<?>> settings =
                Stream.concat(
                        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                        CcrSettings.getSettings().stream().filter(Setting::hasNodeScope))
                        .collect(Collectors.toSet());

        final CcrRepository repository = new CcrRepository(
                repositoryMetaData,
                mock(Client.class),
                new CcrLicenseChecker(() -> true, () -> true),
                Settings.EMPTY,
                new CcrSettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, settings)),
                mock(ThreadPool.class));
        repository.restoreShard(
                mock(IndexShard.class),
                new SnapshotId("snapshot-name", "snapshot-uuid"),
                Version.CURRENT,
                new IndexId("name", "id"),
                new ShardId(new Index("index", "index-uuid"), 0),
                mock(RecoveryState.class));
    }

}