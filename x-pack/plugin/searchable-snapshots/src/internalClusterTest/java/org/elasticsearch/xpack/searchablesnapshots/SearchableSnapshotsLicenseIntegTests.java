/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction;
import org.junit.Before;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotsLicenseIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final String repoName = "test-repo";
    private static final String indexName = "test-index";
    private static final String snapshotName = "test-snapshot";

    @Before
    public void createAndMountSearchableSnapshot() throws Exception {
        createRepository(repoName, "fs");
        createIndex(indexName);
        createFullSnapshot(repoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            indexName,
            repoName,
            snapshotName,
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true,
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        assertAcked(client().execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest()).get());
        assertAcked(client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest()).get());

        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();
    }

    public void testMountRequiresLicense() {
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            indexName + "-extra",
            repoName,
            snapshotName,
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            randomBoolean(),
            randomFrom(MountSearchableSnapshotRequest.Storage.values())
        );

        final ActionFuture<RestoreSnapshotResponse> future = client().execute(MountSearchableSnapshotAction.INSTANCE, req);
        final Throwable cause = ExceptionsHelper.unwrap(expectThrows(Exception.class, future::get), ElasticsearchSecurityException.class);
        assertThat(cause, notNullValue());
        assertThat(cause, instanceOf(ElasticsearchSecurityException.class));
        assertThat(cause.getMessage(), containsString("current license is non-compliant for [searchable-snapshots]"));
    }

    public void testStatsRequiresLicense() throws ExecutionException, InterruptedException {
        final ActionFuture<SearchableSnapshotsStatsResponse> future = client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest(indexName)
        );
        final SearchableSnapshotsStatsResponse response = future.get();
        assertThat(response.getTotalShards(), greaterThan(0));
        assertThat(response.getSuccessfulShards(), equalTo(0));
        for (DefaultShardOperationFailedException shardFailure : response.getShardFailures()) {
            final Throwable cause = ExceptionsHelper.unwrap(shardFailure.getCause(), ElasticsearchSecurityException.class);
            assertThat(cause, notNullValue());
            assertThat(cause, instanceOf(ElasticsearchSecurityException.class));
            assertThat(cause.getMessage(), containsString("current license is non-compliant for [searchable-snapshots]"));
        }
    }

    public void testClearCacheRequiresLicense() throws ExecutionException, InterruptedException {
        final ActionFuture<ClearSearchableSnapshotsCacheResponse> future = client().execute(
            ClearSearchableSnapshotsCacheAction.INSTANCE,
            new ClearSearchableSnapshotsCacheRequest(indexName)
        );
        final ClearSearchableSnapshotsCacheResponse response = future.get();
        assertThat(response.getTotalShards(), greaterThan(0));
        assertThat(response.getSuccessfulShards(), equalTo(0));
        for (DefaultShardOperationFailedException shardFailure : response.getShardFailures()) {
            final Throwable cause = ExceptionsHelper.unwrap(shardFailure.getCause(), ElasticsearchSecurityException.class);
            assertThat(cause, notNullValue());
            assertThat(cause, instanceOf(ElasticsearchSecurityException.class));
            assertThat(cause.getMessage(), containsString("current license is non-compliant for [searchable-snapshots]"));
        }
    }

    @TestLogging(reason = "https://github.com/elastic/elasticsearch/issues/72329", value = "org.elasticsearch.license:DEBUG")
    public void testShardAllocationOnInvalidLicense() throws Exception {
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

        try {
            PostStartTrialRequest startTrialRequest = new PostStartTrialRequest().setType(License.LicenseType.TRIAL.getTypeName())
                .acknowledge(true);
            PostStartTrialResponse resp = client().execute(PostStartTrialAction.INSTANCE, startTrialRequest).get();
            assertEquals(PostStartTrialResponse.Status.UPGRADED_TO_TRIAL, resp.getStatus());
        } catch (AssertionError ae) {
            try {
                final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
                logger.error(
                    "Failed to start trial license again, cluster state on master node is:\n{}",
                    Strings.toString(clusterService.state(), false, true)
                );
            } catch (Exception e) {
                ae.addSuppressed(e);
            }
            throw ae;
        }
        // check if cluster goes green again after valid license has been put in place
        ensureGreen(indexName);
    }

    public void testCachesStatsRequiresLicense() throws Exception {
        final ActionFuture<TransportSearchableSnapshotsNodeCachesStatsAction.NodesCachesStatsResponse> future = client().execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new TransportSearchableSnapshotsNodeCachesStatsAction.NodesRequest(Strings.EMPTY_ARRAY)
        );

        final TransportSearchableSnapshotsNodeCachesStatsAction.NodesCachesStatsResponse response = future.get();
        assertThat(response.failures().size(), equalTo(internalCluster().numDataNodes()));
        assertTrue(response.hasFailures());

        for (FailedNodeException nodeException : response.failures()) {
            final Throwable cause = ExceptionsHelper.unwrap(nodeException.getCause(), ElasticsearchSecurityException.class);
            assertThat(cause, notNullValue());
            assertThat(cause, instanceOf(ElasticsearchSecurityException.class));
            assertThat(cause.getMessage(), containsString("current license is non-compliant for [searchable-snapshots]"));
        }
    }
}
