/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.simulatedlatencyrepo;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class LatencySimulatingBlobStoreRepositoryTests extends ESSingleNodeTestCase {

    public static final String REPO_TYPE = "countingFs";

    public static final LongAdder COUNTS = new LongAdder();

    public static class TestPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                REPO_TYPE,
                metadata -> new LatencySimulatingBlobStoreRepository(
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    COUNTS::increment
                )
            );
        }
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class, LocalStateSearchableSnapshots.class);
    }

    public void testRetrieveSnapshots() throws Exception {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        final String repositoryName = "test-repo";

        logger.info("-->  creating repository");
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType(REPO_TYPE)
            .setSettings(Settings.builder().put(node().settings()).put("location", location))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client().prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        indicesAdmin().prepareFlush(indexName).get();

        logger.info("--> create snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> delete index");
        assertAcked(client.admin().indices().prepareDelete("test-idx").get());

        logger.info("--> mount snapshot");
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            "test-idx",
            repositoryName,
            snapshotId1.getName(),
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        logger.info("--> run a search");
        var searchResponse = client.prepareSearch("test-idx").setQuery(QueryBuilders.termQuery("text", "sometext")).get();

        assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
        assertThat(COUNTS.intValue(), greaterThan(COUNTS.intValue()));
    }
}
