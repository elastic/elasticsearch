/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.archive.ArchiveFeatureSetUsage;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.oneOf;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ArchiveLicenseIntegTests extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateOldLuceneVersions.class, TestRepositoryPlugin.class, MockRepository.Plugin.class);
    }

    public static class TestRepositoryPlugin extends Plugin implements RepositoryPlugin {
        public static final String FAKE_VERSIONS_TYPE = "fakeversionsrepo";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                FAKE_VERSIONS_TYPE,
                metadata -> new FakeVersionsRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }

        // fakes an old index version format to activate license checks
        private static class FakeVersionsRepo extends FsRepository {
            FakeVersionsRepo(
                RepositoryMetadata metadata,
                Environment env,
                NamedXContentRegistry namedXContentRegistry,
                ClusterService clusterService,
                BigArrays bigArrays,
                RecoverySettings recoverySettings
            ) {
                super(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            }

            @Override
            public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index)
                throws IOException {
                final IndexMetadata original = super.getSnapshotIndexMetaData(repositoryData, snapshotId, index);
                return IndexMetadata.builder(original)
                    .settings(
                        Settings.builder()
                            .put(original.getSettings())
                            .put(
                                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                                metadata.settings()
                                    .getAsVersion("version", randomBoolean() ? Version.fromString("5.0.0") : Version.fromString("6.0.0"))
                            )
                    )
                    .build();
            }
        }
    }

    private static final String repoName = "test-repo";
    private static final String indexName = "test-index";
    private static final String snapshotName = "test-snapshot";

    @Before
    public void createAndRestoreArchive() throws Exception {
        createRepository(repoName, TestRepositoryPlugin.FAKE_VERSIONS_TYPE);
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        PostStartTrialRequest request = new PostStartTrialRequest().setType(License.LicenseType.TRIAL.getTypeName()).acknowledge(true);
        client().execute(PostStartTrialAction.INSTANCE, request).get();
    }

    public void testFeatureUsage() throws Exception {
        XPackUsageFeatureResponse usage = client().execute(XPackUsageFeatureAction.ARCHIVE, new XPackUsageRequest()).get();
        assertThat(usage.getUsage(), instanceOf(ArchiveFeatureSetUsage.class));
        ArchiveFeatureSetUsage archiveUsage = (ArchiveFeatureSetUsage) usage.getUsage();
        assertEquals(0, archiveUsage.getNumberOfArchiveIndices());

        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(repoName, snapshotName).indices(indexName).waitForCompletion(true);

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().restoreSnapshot(req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        usage = client().execute(XPackUsageFeatureAction.ARCHIVE, new XPackUsageRequest()).get();
        assertThat(usage.getUsage(), instanceOf(ArchiveFeatureSetUsage.class));
        archiveUsage = (ArchiveFeatureSetUsage) usage.getUsage();
        assertEquals(1, archiveUsage.getNumberOfArchiveIndices());
    }

    public void testFailRestoreOnInvalidLicense() throws Exception {
        assertAcked(client().execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest()).get());
        assertAcked(client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest()).get());

        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();

        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(repoName, snapshotName).indices(indexName).waitForCompletion(true);
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().admin().cluster().restoreSnapshot(req).actionGet()
        );
        assertThat(e.getMessage(), containsString("current license is non-compliant for [archive]"));
    }

    public void testFailRestoreOnTooOldVersion() {
        createRepository(
            repoName,
            TestRepositoryPlugin.FAKE_VERSIONS_TYPE,
            Settings.builder().put(getRepositoryOnMaster(repoName).getMetadata().settings()).put("version", Version.fromString("2.0.0").id)
        );
        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(repoName, snapshotName).indices(indexName).waitForCompletion(true);
        SnapshotRestoreException e = expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin().cluster().restoreSnapshot(req).actionGet()
        );
        assertThat(
            e.getMessage(),
            containsString(
                "the snapshot was created with Elasticsearch version [2.0.0] " + "which isn't supported by the archive functionality"
            )
        );
    }

    // checks that shards are failed if license becomes invalid after successful restore
    public void testShardAllocationOnInvalidLicense() throws Exception {
        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(repoName, snapshotName).indices(indexName).waitForCompletion(true);

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().restoreSnapshot(req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        assertAcked(client().execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest()).get());
        assertAcked(client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest()).get());

        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();

        // check that shards have been failed as part of invalid license
        assertBusy(
            () -> assertEquals(
                ClusterHealthStatus.RED,
                client().admin().cluster().prepareHealth(indexName).get().getIndices().get(indexName).getStatus()
            )
        );

        waitNoPendingTasksOnAll();
        ensureClusterStateConsistency();

        // add a valid license again
        // This is a bit of a hack in tests, as we can't readd a trial license
        // We force this by clearing the existing basic license first
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata()).removeCustom(LicensesMetadata.TYPE).build())
                .build()
        );

        waitNoPendingTasksOnAll();
        ensureClusterStateConsistency();

        PostStartTrialRequest request = new PostStartTrialRequest().setType(License.LicenseType.TRIAL.getTypeName()).acknowledge(true);
        final PostStartTrialResponse response = client().execute(PostStartTrialAction.INSTANCE, request).get();
        assertThat(
            response.getStatus(),
            oneOf(
                PostStartTrialResponse.Status.UPGRADED_TO_TRIAL,
                // The LicenceService automatically generates a license of {@link LicenceService#SELF_GENERATED_LICENSE_TYPE} type
                // if there is no license found in the cluster state (see {@link LicenceService#registerOrUpdateSelfGeneratedLicense).
                // Since this test explicitly removes the LicensesMetadata from cluster state it is possible that the self generated
                // license is created before the PostStartTrialRequest is acked.
                PostStartTrialResponse.Status.TRIAL_ALREADY_ACTIVATED
            )
        );
        // check if cluster goes green again after valid license has been put in place
        ensureGreen(indexName);
    }
}
