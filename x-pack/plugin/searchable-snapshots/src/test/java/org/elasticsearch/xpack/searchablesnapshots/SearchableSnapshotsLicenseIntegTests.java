/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.protocol.xpack.license.DeleteLicenseRequest;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;
import org.junit.Before;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotsLicenseIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    private static final String repoName = "test-repo";
    private static final String indexName = "test-index";
    private static final String snapshotName = "test-snapshot";

    @Before
    public void createAndMountSearchableSnapshot() throws Exception {
        final Path repo = randomRepoPath();
        assertAcked(
            client().admin().cluster().preparePutRepository(repoName).setType("fs").setSettings(Settings.builder().put("location", repo))
        );

        createIndex(indexName);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final Settings.Builder indexSettingsBuilder = Settings.builder().put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false);
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            indexName,
            repoName,
            snapshotName,
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        assertAcked(client().execute(DeleteLicenseAction.INSTANCE, new DeleteLicenseRequest()).get());
        assertAcked(client().execute(PostStartBasicAction.INSTANCE, new PostStartBasicRequest()).get());
    }

    public void testMountRequiresLicense() {
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            indexName + "-extra",
            repoName,
            snapshotName,
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            randomBoolean()
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
}
