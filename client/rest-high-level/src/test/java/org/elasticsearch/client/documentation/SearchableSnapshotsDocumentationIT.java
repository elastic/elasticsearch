/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.searchable_snapshots.CachesStatsRequest;
import org.elasticsearch.client.searchable_snapshots.CachesStatsResponse;
import org.elasticsearch.client.searchable_snapshots.CachesStatsResponse.NodeCachesStats;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@SuppressWarnings("removal")
public class SearchableSnapshotsDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testMountSnapshot() throws IOException, InterruptedException {
        final RestHighLevelClient client = highLevelClient();
        {
            final CreateIndexRequest request = new CreateIndexRequest("index").settings(
                Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
            );
            final CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            assertTrue(response.isAcknowledged());
        }

        {
            final IndexRequest request = new IndexRequest("index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source("{}", XContentType.JSON);
            final IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            assertThat(response.status(), is(RestStatus.CREATED));
        }

        {
            final PutRepositoryRequest request = new PutRepositoryRequest("repository");
            request.settings("{\"location\": \".\"}", XContentType.JSON);
            request.type(FsRepository.TYPE);
            final AcknowledgedResponse response = client.snapshot().createRepository(request, RequestOptions.DEFAULT);
            assertTrue(response.isAcknowledged());
        }

        {
            final CreateSnapshotRequest request = new CreateSnapshotRequest("repository", "snapshot").waitForCompletion(true);
            final CreateSnapshotResponse response = client.snapshot().create(request, RequestOptions.DEFAULT);
            assertThat(response.getSnapshotInfo().status(), is(RestStatus.OK));
        }

        // tag::searchable-snapshots-mount-snapshot-request
        final MountSnapshotRequest request = new MountSnapshotRequest(
            "repository", // <1>
            "snapshot", // <2>
            "index" // <3>
        );
        request.masterTimeout(TimeValue.timeValueSeconds(30)); // <4>
        request.waitForCompletion(true); // <5>
        request.storage(MountSnapshotRequest.Storage.FULL_COPY); // <6>
        request.renamedIndex("renamed_index"); // <7>
        final Settings indexSettings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .build();
        request.indexSettings(indexSettings); // <8>
        request.ignoreIndexSettings(
            new String[]{"index.refresh_interval"}); // <9>
        // end::searchable-snapshots-mount-snapshot-request

        // tag::searchable-snapshots-mount-snapshot-execute
        final RestoreSnapshotResponse response = client
            .searchableSnapshots()
            .mountSnapshot(request, RequestOptions.DEFAULT);
        // end::searchable-snapshots-mount-snapshot-execute

        Map<String, Object> settings = getIndexSettings("renamed_index", true);
        @SuppressWarnings("unchecked")
        Map<String, Object> renamedIndexSettings = (Map<String, Object>) settings.get("renamed_index");
        @SuppressWarnings("unchecked")
        Map<String, Object> defaultIndexSettings = (Map<String, Object>) renamedIndexSettings.get("defaults");

        assertThat(
            "Original index refresh interval setting should have been ignored when mounting the index: " + settings,
            defaultIndexSettings.get(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()),
            equalTo("1s")
        );

        // tag::searchable-snapshots-mount-snapshot-response
        final RestoreInfo restoreInfo = response.getRestoreInfo(); // <1>
        // end::searchable-snapshots-mount-snapshot-response

        // tag::searchable-snapshots-mount-snapshot-execute-listener
        ActionListener<RestoreSnapshotResponse> listener =
            new ActionListener<RestoreSnapshotResponse>() {

                @Override
                public void onResponse(
                    final RestoreSnapshotResponse response) { // <1>
                    final RestoreInfo restoreInfo = response.getRestoreInfo();
                }

                @Override
                public void onFailure(final Exception e) {
                    // <2>
                }

            };
        // end::searchable-snapshots-mount-snapshot-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::searchable-snapshots-mount-snapshot-execute-async
        client.searchableSnapshots().mountSnapshotAsync(
            request,
            RequestOptions.DEFAULT,
            listener // <1>
        );
        // end::searchable-snapshots-mount-snapshot-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testCachesStatsSnapshot() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        // tag::searchable-snapshots-caches-stats-request
        CachesStatsRequest request = new CachesStatsRequest(); // <1>
        request = new CachesStatsRequest(  // <2>
            "eerrtBMtQEisohZzxBLUSw",
            "klksqQSSzASDqDMLQ"
        );
        // end::searchable-snapshots-caches-stats-request

        // tag::searchable-snapshots-caches-stats-execute
        final CachesStatsResponse response = client
            .searchableSnapshots()
            .cacheStats(request, RequestOptions.DEFAULT);
        // end::searchable-snapshots-caches-stats-execute

        // tag::searchable-snapshots-caches-stats-response
        final List<NodeCachesStats> nodeCachesStats =
            response.getNodeCachesStats(); // <1>
        // end::searchable-snapshots-caches-stats-response

        // tag::searchable-snapshots-caches-stats-execute-listener
        ActionListener<CachesStatsResponse> listener =
            new ActionListener<CachesStatsResponse>() {

                @Override
                public void onResponse(final CachesStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(final Exception e) {
                    // <2>
                }
            };
        // end::searchable-snapshots-caches-stats-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::searchable-snapshots-caches-stats-execute-async
        client.searchableSnapshots().cacheStatsAsync(
            request,
            RequestOptions.DEFAULT,
            listener // <1>
        );
        // end::searchable-snapshots-caches-stats-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

}
