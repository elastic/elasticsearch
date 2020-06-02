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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * This class is used to generate the Java Snapshot API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/SnapshotClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class SnapshotClientDocumentationIT extends ESRestHighLevelClientTestCase {

    private static final String repositoryName = "test_repository";
    private static final String snapshotName = "test_snapshot";
    private static final String indexName = "test_index";

    @Override
    protected boolean waitForAllSnapshotsWiped() {
        return true;
    }

    public void testSnapshotCreateRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::create-repository-request
        PutRepositoryRequest request = new PutRepositoryRequest();
        // end::create-repository-request

        // tag::create-repository-create-settings
        String locationKey = FsRepository.LOCATION_SETTING.getKey();
        String locationValue = ".";
        String compressKey = FsRepository.COMPRESS_SETTING.getKey();
        boolean compressValue = true;

        Settings settings = Settings.builder()
            .put(locationKey, locationValue)
            .put(compressKey, compressValue)
            .build(); // <1>
        // end::create-repository-create-settings

        // tag::create-repository-request-repository-settings
        request.settings(settings); // <1>
        // end::create-repository-request-repository-settings

        {
            // tag::create-repository-settings-builder
            Settings.Builder settingsBuilder = Settings.builder()
                .put(locationKey, locationValue)
                .put(compressKey, compressValue);
            request.settings(settingsBuilder); // <1>
            // end::create-repository-settings-builder
        }
        {
            // tag::create-repository-settings-map
            Map<String, Object> map = new HashMap<>();
            map.put(locationKey, locationValue);
            map.put(compressKey, compressValue);
            request.settings(map); // <1>
            // end::create-repository-settings-map
        }
        {
            // tag::create-repository-settings-source
            request.settings("{\"location\": \".\", \"compress\": \"true\"}",
                XContentType.JSON); // <1>
            // end::create-repository-settings-source
        }

        // tag::create-repository-request-name
        request.name(repositoryName); // <1>
        // end::create-repository-request-name
        // tag::create-repository-request-type
        request.type(FsRepository.TYPE); // <1>
        // end::create-repository-request-type

        // tag::create-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::create-repository-request-masterTimeout
        // tag::create-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::create-repository-request-timeout
        // tag::create-repository-request-verify
        request.verify(true); // <1>
        // end::create-repository-request-verify

        // tag::create-repository-execute
        AcknowledgedResponse response = client.snapshot().createRepository(request, RequestOptions.DEFAULT);
        // end::create-repository-execute

        // tag::create-repository-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::create-repository-response
        assertTrue(acknowledged);
    }

    public void testSnapshotCreateRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            PutRepositoryRequest request = new PutRepositoryRequest(repositoryName);

            // tag::create-repository-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse putRepositoryResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::create-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::create-repository-execute-async
            client.snapshot().createRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::create-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotGetRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();

        // tag::get-repository-request
        GetRepositoriesRequest request = new GetRepositoriesRequest();
        // end::get-repository-request

        // tag::get-repository-request-repositories
        String [] repositories = new String[] {repositoryName};
        request.repositories(repositories); // <1>
        // end::get-repository-request-repositories
        // tag::get-repository-request-local
        request.local(true); // <1>
        // end::get-repository-request-local
        // tag::get-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::get-repository-request-masterTimeout

        // tag::get-repository-execute
        GetRepositoriesResponse response = client.snapshot().getRepository(request, RequestOptions.DEFAULT);
        // end::get-repository-execute

        // tag::get-repository-response
        List<RepositoryMetadata> repositoryMetadataResponse = response.repositories();
        // end::get-repository-response
        assertThat(1, equalTo(repositoryMetadataResponse.size()));
        assertThat(repositoryName, equalTo(repositoryMetadataResponse.get(0).name()));
    }

    public void testSnapshotGetRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            GetRepositoriesRequest request = new GetRepositoriesRequest();

            // tag::get-repository-execute-listener
            ActionListener<GetRepositoriesResponse> listener =
                    new ActionListener<GetRepositoriesResponse>() {
                @Override
                public void onResponse(GetRepositoriesResponse getRepositoriesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-repository-execute-async
            client.snapshot().getRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testRestoreSnapshot() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();
        createTestIndex();
        createTestSnapshots();

        // tag::restore-snapshot-request
        RestoreSnapshotRequest request = new RestoreSnapshotRequest(repositoryName, snapshotName);
        // end::restore-snapshot-request
        // we need to restore as a different index name

        // tag::restore-snapshot-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::restore-snapshot-request-masterTimeout

        // tag::restore-snapshot-request-waitForCompletion
        request.waitForCompletion(true); // <1>
        // end::restore-snapshot-request-waitForCompletion

        // tag::restore-snapshot-request-partial
        request.partial(false); // <1>
        // end::restore-snapshot-request-partial

        // tag::restore-snapshot-request-include-global-state
        request.includeGlobalState(false); // <1>
        // end::restore-snapshot-request-include-global-state

        // tag::restore-snapshot-request-include-aliases
        request.includeAliases(false); // <1>
        // end::restore-snapshot-request-include-aliases


        // tag::restore-snapshot-request-indices
        request.indices("test_index"); // <1>
        // end::restore-snapshot-request-indices

        String restoredIndexName = "restored_index";
        // tag::restore-snapshot-request-rename
        request.renamePattern("test_(.+)"); // <1>
        request.renameReplacement("restored_$1"); // <2>
        // end::restore-snapshot-request-rename

        // tag::restore-snapshot-request-index-settings
        request.indexSettings(  // <1>
            Settings.builder()
            .put("index.number_of_replicas", 0)
                .build());

        request.ignoreIndexSettings("index.refresh_interval", "index.search.idle.after"); // <2>
        request.indicesOptions(new IndicesOptions( // <3>
            EnumSet.of(IndicesOptions.Option.IGNORE_UNAVAILABLE),
            EnumSet.of(IndicesOptions.WildcardStates.OPEN)));
        // end::restore-snapshot-request-index-settings

        // tag::restore-snapshot-execute
        RestoreSnapshotResponse response = client.snapshot().restore(request, RequestOptions.DEFAULT);
        // end::restore-snapshot-execute

        // tag::restore-snapshot-response
        RestoreInfo restoreInfo = response.getRestoreInfo();
        List<String> indices = restoreInfo.indices(); // <1>
        // end::restore-snapshot-response
        assertEquals(Collections.singletonList(restoredIndexName), indices);
        assertEquals(0, restoreInfo.failedShards());
        assertTrue(restoreInfo.successfulShards() > 0);
    }

    public void testRestoreSnapshotAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            RestoreSnapshotRequest request = new RestoreSnapshotRequest();

            // tag::restore-snapshot-execute-listener
            ActionListener<RestoreSnapshotResponse> listener =
                new ActionListener<RestoreSnapshotResponse>() {
                    @Override
                    public void onResponse(RestoreSnapshotResponse restoreSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::restore-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::restore-snapshot-execute-async
            client.snapshot().restoreAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::restore-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotDeleteRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();

        // tag::delete-repository-request
        DeleteRepositoryRequest request = new DeleteRepositoryRequest(repositoryName);
        // end::delete-repository-request

        // tag::delete-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::delete-repository-request-masterTimeout
        // tag::delete-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::delete-repository-request-timeout

        // tag::delete-repository-execute
        AcknowledgedResponse response = client.snapshot().deleteRepository(request, RequestOptions.DEFAULT);
        // end::delete-repository-execute

        // tag::delete-repository-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::delete-repository-response
        assertTrue(acknowledged);
    }

    public void testSnapshotDeleteRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            DeleteRepositoryRequest request = new DeleteRepositoryRequest();

            // tag::delete-repository-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse deleteRepositoryResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-repository-execute-async
            client.snapshot().deleteRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotVerifyRepository() throws IOException {
        RestHighLevelClient client = highLevelClient();
        createTestRepositories();

        // tag::verify-repository-request
        VerifyRepositoryRequest request = new VerifyRepositoryRequest(repositoryName);
        // end::verify-repository-request

        // tag::verify-repository-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::verify-repository-request-masterTimeout
        // tag::verify-repository-request-timeout
        request.timeout(TimeValue.timeValueMinutes(1)); // <1>
        request.timeout("1m"); // <2>
        // end::verify-repository-request-timeout

        // tag::verify-repository-execute
        VerifyRepositoryResponse response = client.snapshot().verifyRepository(request, RequestOptions.DEFAULT);
        // end::verify-repository-execute

        // tag::verify-repository-response
        List<VerifyRepositoryResponse.NodeView> repositoryMetadataResponse = response.getNodes();
        // end::verify-repository-response
        assertThat(1, equalTo(repositoryMetadataResponse.size()));
        final boolean async = Booleans.parseBoolean(System.getProperty("tests.rest.async", "false"));
        if (async) {
            assertThat("asyncIntegTest-0", equalTo(repositoryMetadataResponse.get(0).getName()));
        } else {
            assertThat("integTest-0", equalTo(repositoryMetadataResponse.get(0).getName()));
        }
    }

    public void testSnapshotVerifyRepositoryAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            VerifyRepositoryRequest request = new VerifyRepositoryRequest(repositoryName);

            // tag::verify-repository-execute-listener
            ActionListener<VerifyRepositoryResponse> listener =
                new ActionListener<VerifyRepositoryResponse>() {
                    @Override
                    public void onResponse(VerifyRepositoryResponse verifyRepositoryRestResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::verify-repository-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::verify-repository-execute-async
            client.snapshot().verifyRepositoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::verify-repository-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotCreate() throws IOException {
        RestHighLevelClient client = highLevelClient();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test-index0");
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        createIndexRequest = new CreateIndexRequest("test-index1");
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        createTestRepositories();

        // tag::create-snapshot-request
        CreateSnapshotRequest request = new CreateSnapshotRequest();
        // end::create-snapshot-request

        // tag::create-snapshot-request-repositoryName
        request.repository(repositoryName); // <1>
        // end::create-snapshot-request-repositoryName
        // tag::create-snapshot-request-snapshotName
        request.snapshot(snapshotName); // <1>
        // end::create-snapshot-request-snapshotName
        // tag::create-snapshot-request-indices
        request.indices("test-index0", "test-index1"); // <1>
        // end::create-snapshot-request-indices
        // tag::create-snapshot-request-indicesOptions
        request.indicesOptions(IndicesOptions.fromOptions(false, false, true, true)); // <1>
        // end::create-snapshot-request-indicesOptions
        // tag::create-snapshot-request-partial
        request.partial(false); // <1>
        // end::create-snapshot-request-partial
        // tag::create-snapshot-request-includeGlobalState
        request.includeGlobalState(true); // <1>
        // end::create-snapshot-request-includeGlobalState

        // tag::create-snapshot-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::create-snapshot-request-masterTimeout
        // tag::create-snapshot-request-waitForCompletion
        request.waitForCompletion(true); // <1>
        // end::create-snapshot-request-waitForCompletion

        // tag::create-snapshot-execute
        CreateSnapshotResponse response = client.snapshot().create(request, RequestOptions.DEFAULT);
        // end::create-snapshot-execute

        // tag::create-snapshot-response
        RestStatus status = response.status(); // <1>
        // end::create-snapshot-response

        assertEquals(RestStatus.OK, status);

        // tag::create-snapshot-response-snapshot-info
        SnapshotInfo snapshotInfo = response.getSnapshotInfo(); // <1>
        // end::create-snapshot-response-snapshot-info

        assertNotNull(snapshotInfo);
    }

    public void testSnapshotCreateAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            CreateSnapshotRequest request = new CreateSnapshotRequest(repositoryName, snapshotName);

            // tag::create-snapshot-execute-listener
            ActionListener<CreateSnapshotResponse> listener =
                new ActionListener<CreateSnapshotResponse>() {
                    @Override
                    public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        // <2>
                    }
                };
            // end::create-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::create-snapshot-execute-async
            client.snapshot().createAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::create-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testSnapshotGetSnapshots() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();
        createTestIndex();
        createTestSnapshots();

        // tag::get-snapshots-request
        GetSnapshotsRequest request = new GetSnapshotsRequest();
        // end::get-snapshots-request

        // tag::get-snapshots-request-repositoryName
        request.repositories(repositoryName); // <1>
        // end::get-snapshots-request-repositoryName

        // tag::get-snapshots-request-snapshots
        String[] snapshots = { snapshotName };
        request.snapshots(snapshots); // <1>
        // end::get-snapshots-request-snapshots

        // tag::get-snapshots-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::get-snapshots-request-masterTimeout

        // tag::get-snapshots-request-verbose
        request.verbose(true); // <1>
        // end::get-snapshots-request-verbose

        // tag::get-snapshots-request-ignore-unavailable
        request.ignoreUnavailable(false); // <1>
        // end::get-snapshots-request-ignore-unavailable

        // tag::get-snapshots-execute
        GetSnapshotsResponse response = client.snapshot().get(request, RequestOptions.DEFAULT);
        // end::get-snapshots-execute

        // tag::get-snapshots-response
        List<SnapshotInfo> snapshotsInfos = response.getSnapshots(repositoryName);
        SnapshotInfo snapshotInfo = snapshotsInfos.get(0);
        RestStatus restStatus = snapshotInfo.status(); // <1>
        SnapshotId snapshotId = snapshotInfo.snapshotId(); // <2>
        SnapshotState snapshotState = snapshotInfo.state(); // <3>
        List<SnapshotShardFailure> snapshotShardFailures = snapshotInfo.shardFailures(); // <4>
        long startTime = snapshotInfo.startTime(); // <5>
        long endTime = snapshotInfo.endTime(); // <6>
        // end::get-snapshots-response
        assertEquals(1, snapshotsInfos.size());
    }

    public void testSnapshotGetSnapshotsAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            GetSnapshotsRequest request = new GetSnapshotsRequest(repositoryName);

            // tag::get-snapshots-execute-listener
            ActionListener<GetSnapshotsResponse> listener =
                new ActionListener<GetSnapshotsResponse>() {
                    @Override
                    public void onResponse(GetSnapshotsResponse getSnapshotsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-snapshots-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-snapshots-execute-async
            client.snapshot().getAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-snapshots-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotSnapshotsStatus() throws IOException {
        RestHighLevelClient client = highLevelClient();
        createTestRepositories();
        createTestIndex();
        createTestSnapshots();

        // tag::snapshots-status-request
        SnapshotsStatusRequest request = new SnapshotsStatusRequest();
        // end::snapshots-status-request

        // tag::snapshots-status-request-repository
        request.repository(repositoryName); // <1>
        // end::snapshots-status-request-repository
        // tag::snapshots-status-request-snapshots
        String [] snapshots = new String[] {snapshotName};
        request.snapshots(snapshots); // <1>
        // end::snapshots-status-request-snapshots
        // tag::snapshots-status-request-ignoreUnavailable
        request.ignoreUnavailable(true); // <1>
        // end::snapshots-status-request-ignoreUnavailable
        // tag::snapshots-status-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::snapshots-status-request-masterTimeout

        // tag::snapshots-status-execute
        SnapshotsStatusResponse response = client.snapshot().status(request, RequestOptions.DEFAULT);
        // end::snapshots-status-execute

        // tag::snapshots-status-response
        List<SnapshotStatus> snapshotStatusesResponse = response.getSnapshots();
        SnapshotStatus snapshotStatus = snapshotStatusesResponse.get(0); // <1>
        SnapshotsInProgress.State snapshotState = snapshotStatus.getState(); // <2>
        SnapshotStats shardStats = snapshotStatus.getIndices().get(indexName).getShards().get(0).getStats(); // <3>
        // end::snapshots-status-response
        assertThat(snapshotStatusesResponse.size(), equalTo(1));
        assertThat(snapshotStatusesResponse.get(0).getSnapshot().getRepository(), equalTo(SnapshotClientDocumentationIT.repositoryName));
        assertThat(snapshotStatusesResponse.get(0).getSnapshot().getSnapshotId().getName(), equalTo(snapshotName));
        assertThat(snapshotState.completed(), equalTo(true));
    }

    public void testSnapshotSnapshotsStatusAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            SnapshotsStatusRequest request = new SnapshotsStatusRequest();

            // tag::snapshots-status-execute-listener
            ActionListener<SnapshotsStatusResponse> listener =
                new ActionListener<SnapshotsStatusResponse>() {
                    @Override
                    public void onResponse(SnapshotsStatusResponse snapshotsStatusResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::snapshots-status-execute-listener

            // Replace the empty listener with a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::snapshots-status-execute-async
            client.snapshot().statusAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::snapshots-status-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSnapshotDeleteSnapshot() throws IOException {
        RestHighLevelClient client = highLevelClient();

        createTestRepositories();
        createTestIndex();
        createTestSnapshots();

        // tag::delete-snapshot-request
        DeleteSnapshotRequest request = new DeleteSnapshotRequest(repositoryName);
        request.snapshots(snapshotName);
        // end::delete-snapshot-request

        // tag::delete-snapshot-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::delete-snapshot-request-masterTimeout

        // tag::delete-snapshot-execute
        AcknowledgedResponse response = client.snapshot().delete(request, RequestOptions.DEFAULT);
        // end::delete-snapshot-execute

        // tag::delete-snapshot-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::delete-snapshot-response
        assertTrue(acknowledged);
    }

    public void testSnapshotDeleteSnapshotAsync() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            DeleteSnapshotRequest request = new DeleteSnapshotRequest();

            // tag::delete-snapshot-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse deleteSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-snapshot-execute-async
            client.snapshot().deleteAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private void createTestRepositories() throws IOException {
        PutRepositoryRequest request = new PutRepositoryRequest(repositoryName);
        request.type(FsRepository.TYPE);
        request.settings("{\"location\": \".\"}", XContentType.JSON);
        assertTrue(highLevelClient().snapshot().createRepository(request, RequestOptions.DEFAULT).isAcknowledged());
    }

    private void createTestIndex() throws IOException {
        createIndex(indexName, Settings.EMPTY);
    }

    private void createTestSnapshots() throws IOException {
        Request createSnapshot = new Request("put", String.format(Locale.ROOT, "_snapshot/%s/%s", repositoryName, snapshotName));
        createSnapshot.addParameter("wait_for_completion", "true");
        createSnapshot.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        Response response = highLevelClient().getLowLevelClient().performRequest(createSnapshot);
        // check that the request went ok without parsing JSON here. When using the high level client, check acknowledgement instead.
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
