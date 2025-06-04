/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.TransportDeleteLicenseAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;
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
import static org.hamcrest.Matchers.oneOf;

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

        assertAcked(indicesAdmin().prepareDelete(indexName));

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            TEST_REQUEST_TIMEOUT,
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

        assertAcked(
            client().execute(TransportDeleteLicenseAction.TYPE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
                .get()
        );
        assertAcked(
            client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get()
        );

        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();
    }

    public void testMountRequiresLicense() {
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            TEST_REQUEST_TIMEOUT,
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
        final ActionFuture<BroadcastResponse> future = client().execute(
            ClearSearchableSnapshotsCacheAction.INSTANCE,
            new ClearSearchableSnapshotsCacheRequest(indexName)
        );
        final BroadcastResponse response = future.get();
        assertThat(response.getTotalShards(), greaterThan(0));
        assertThat(response.getSuccessfulShards(), equalTo(0));
        for (DefaultShardOperationFailedException shardFailure : response.getShardFailures()) {
            final Throwable cause = ExceptionsHelper.unwrap(shardFailure.getCause(), ElasticsearchSecurityException.class);
            assertThat(cause, notNullValue());
            assertThat(cause, instanceOf(ElasticsearchSecurityException.class));
            assertThat(cause.getMessage(), containsString("current license is non-compliant for [searchable-snapshots]"));
        }
    }

    public void testShardAllocationOnInvalidLicense() throws Exception {
        // check that shards have been failed as part of invalid license
        assertBusy(
            () -> assertEquals(
                ClusterHealthStatus.RED,
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getIndices().get(indexName).getStatus()
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

        PostStartTrialRequest request = new PostStartTrialRequest(TEST_REQUEST_TIMEOUT).setType(License.LicenseType.TRIAL.getTypeName())
            .acknowledge(true);
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
