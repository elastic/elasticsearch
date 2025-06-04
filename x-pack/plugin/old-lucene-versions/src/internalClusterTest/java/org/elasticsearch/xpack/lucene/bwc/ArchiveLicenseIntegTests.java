/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.TransportDeleteLicenseAction;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.archive.ArchiveFeatureSetUsage;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.oneOf;

public class ArchiveLicenseIntegTests extends AbstractArchiveTestCase {

    public void testFeatureUsage() throws Exception {
        XPackUsageFeatureResponse usage = safeGet(
            client().execute(XPackUsageFeatureAction.ARCHIVE, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT))
        );
        assertThat(usage.getUsage(), instanceOf(ArchiveFeatureSetUsage.class));
        ArchiveFeatureSetUsage archiveUsage = (ArchiveFeatureSetUsage) usage.getUsage();
        assertEquals(0, archiveUsage.getNumberOfArchiveIndices());

        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).indices(indexName)
            .waitForCompletion(true);

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().restoreSnapshot(req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        usage = safeGet(client().execute(XPackUsageFeatureAction.ARCHIVE, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT)));
        assertThat(usage.getUsage(), instanceOf(ArchiveFeatureSetUsage.class));
        archiveUsage = (ArchiveFeatureSetUsage) usage.getUsage();
        assertEquals(1, archiveUsage.getNumberOfArchiveIndices());
    }

    public void testFailRestoreOnInvalidLicense() throws Exception {
        assertAcked(
            client().execute(TransportDeleteLicenseAction.TYPE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
                .get()
        );
        assertAcked(
            client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get()
        );

        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();

        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).indices(indexName)
            .waitForCompletion(true);
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> clusterAdmin().restoreSnapshot(req).actionGet()
        );
        assertThat(e.getMessage(), containsString("current license is non-compliant for [archive]"));
    }

    public void testFailRestoreOnTooOldVersion() {
        createRepository(
            repoName,
            TestRepositoryPlugin.FAKE_VERSIONS_TYPE,
            Settings.builder().put(getRepositoryOnMaster(repoName).getMetadata().settings()).put("version", Version.fromString("2.0.0").id)
        );
        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).indices(indexName)
            .waitForCompletion(true);
        SnapshotRestoreException e = expectThrows(SnapshotRestoreException.class, () -> clusterAdmin().restoreSnapshot(req).actionGet());
        assertThat(
            e.getMessage(),
            containsString("the snapshot has indices of version [2000099] which isn't supported by the archive functionality")
        );
    }

    // checks that shards are failed if license becomes invalid after successful restore
    public void testShardAllocationOnInvalidLicense() throws Exception {
        final RestoreSnapshotRequest req = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshotName).indices(indexName)
            .waitForCompletion(true);

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().restoreSnapshot(req).get();
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
}
