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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamsSnapshotsIT extends AbstractSnapshotIntegTestCase {

    private static final String DS_BACKING_INDEX_NAME = DataStream.getDefaultBackingIndexName("ds", 1);
    private static final String DS2_BACKING_INDEX_NAME = DataStream.getDefaultBackingIndexName("ds2", 1);
    private static final Map<String, Integer> DOCUMENT_SOURCE = Collections.singletonMap("@timestamp", 123);
    public static final String REPO = "repo";
    public static final String SNAPSHOT = "snap";
    private Client client;

    private String id;

    @Before
    public void setup() throws Exception {
        client = client();
        Path location = randomRepoPath();
        createRepository(REPO, "fs", location);

        DataStreamIT.putComposableIndexTemplate("t1", "@timestamp", List.of("ds", "other-ds"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        request = new CreateDataStreamAction.Request("other-ds");
        response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        IndexResponse indexResponse = client.prepareIndex("ds")
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(DOCUMENT_SOURCE)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        id = indexResponse.getId();
    }

    public void testSnapshotAndRestore() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        GetSnapshotsResponse snapshot = client.admin().cluster().prepareGetSnapshots(REPO).setSnapshots(SNAPSHOT).get();
        List<SnapshotInfo> snap = snapshot.getSnapshots(REPO);
        assertEquals(1, snap.size());
        assertEquals(Collections.singletonList(DS_BACKING_INDEX_NAME), snap.get(0).indices());

        assertTrue(client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"ds"})).get()
            .isAcknowledged());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS_BACKING_INDEX_NAME, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
    }

    public void testSnapshotAndRestoreAll() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
                .prepareCreateSnapshot(REPO, SNAPSHOT)
                .setWaitForCompletion(true)
                .setIndices("ds")
                .setIncludeGlobalState(false)
                .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        GetSnapshotsResponse snapshot = client.admin().cluster().prepareGetSnapshots(REPO).setSnapshots(SNAPSHOT).get();
        List<SnapshotInfo> snap = snapshot.getSnapshots(REPO);
        assertEquals(1, snap.size());
        assertEquals(Collections.singletonList(DS_BACKING_INDEX_NAME), snap.get(0).indices());

        assertAcked(client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"*"})).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
                .prepareRestoreSnapshot(REPO, SNAPSHOT)
                .setWaitForCompletion(true)
                .setRestoreGlobalState(true)
                .get();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS_BACKING_INDEX_NAME, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());

        assertAcked(client().admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"ds"})).get());
    }

    public void testRename() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(SnapshotRestoreException.class, () -> client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get());

        client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds2")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS2_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertEquals(DOCUMENT_SOURCE, client.prepareSearch("ds2").get().getHits().getHits()[0].getSourceAsMap());
        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS2_BACKING_INDEX_NAME, id).get().getSourceAsMap());
    }

    public void testBackingIndexIsNotRenamedWhenRestoringDataStream() {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(SnapshotRestoreException.class, () -> client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get());

        // delete data stream
        client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"ds"})).actionGet();

        // restore data stream attempting to rename the backing index
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern(DS_BACKING_INDEX_NAME)
            .setRenameReplacement("new_index_name")
            .get();

        assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));

        GetDataStreamAction.Request getDSRequest = new GetDataStreamAction.Request("ds");
        GetDataStreamAction.Response response = client.admin().indices().getDataStreams(getDSRequest).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(DS_BACKING_INDEX_NAME));
    }

    public void testDataStreamAndBackingIndidcesAreRenamedUsingRegex() {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(SnapshotRestoreException.class, () -> client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get());

        // restore data stream attempting to rename the backing index
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern("(.+)")
            .setRenameReplacement("test-$1")
            .get();

        assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));

        // assert "ds" was restored as "test-ds" and the backing index has a valid name
        GetDataStreamAction.Request getRenamedDS = new GetDataStreamAction.Request("test-ds");
        GetDataStreamAction.Response response = client.admin().indices().getDataStreams(getRenamedDS).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(),
            is(DataStream.getDefaultBackingIndexName("test-ds", 1L)));

        // data stream "ds" should still exist in the system
        GetDataStreamAction.Request getDSRequest = new GetDataStreamAction.Request("ds");
        response = client.admin().indices().getDataStreams(getDSRequest).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(DS_BACKING_INDEX_NAME));
    }

    public void testWildcards() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds2")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS2_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertThat("we renamed the restored data stream to one that doesn't match any existing composable template",
            ds.getDataStreams().get(0).getIndexTemplate(), is(nullValue()));
    }

    public void testDataStreamNotStoredWhenIndexRequested() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices(DS_BACKING_INDEX_NAME)
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        expectThrows(Exception.class, () -> client.admin().cluster()
            .prepareRestoreSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get());
    }

    public void testDataStreamNotRestoredWhenIndexRequested() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        assertTrue(client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"ds"})).get()
            .isAcknowledged());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices(".ds-ds-*")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Request getRequest = new GetDataStreamAction.Request("ds");
        expectThrows(ResourceNotFoundException.class, () -> client.admin().indices().getDataStreams(getRequest).actionGet());
    }

    public void testDataStreamNotIncludedInLimitedSnapshot() throws ExecutionException, InterruptedException {
        final String snapshotName = "test-snap";
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
                .prepareCreateSnapshot(REPO, snapshotName)
                .setWaitForCompletion(true)
                .setIndices("does-not-exist-*")
                .setIncludeGlobalState(true)
                .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), Matchers.is(SnapshotState.SUCCESS));

        assertThat(client().admin().indices()
                .deleteDataStream(new DeleteDataStreamAction.Request(new String[]{"*"})).get().isAcknowledged(), is(true));

        final RestoreSnapshotResponse restoreSnapshotResponse =
                client().admin().cluster().prepareRestoreSnapshot(REPO, snapshotName).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), empty());
    }

    public void testDeleteDataStreamDuringSnapshot() throws Exception {
        Client client = client();

        createRepository("test-repo", "mock", Settings.builder()
            .put("location", randomRepoPath()).put("compress", randomBoolean())
            .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
            .put("block_on_data", true));


        String dataStream = "datastream";
        DataStreamIT.putComposableIndexTemplate("dst", "@timestamp", List.of(dataStream));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client.prepareIndex(dataStream)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setId(Integer.toString(i))
                .setSource(Collections.singletonMap("@timestamp", "2020-12-12"))
                .execute().actionGet();
        }
        refresh();
        assertDocCount(dataStream, 100L);

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> future = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices(dataStream).setWaitForCompletion(true).setPartial(false).execute();
        logger.info("--> wait for block to kick in");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

        // non-partial snapshots do not allow delete operations on data streams where snapshot has not been completed
        try {
            logger.info("--> delete index while non-partial snapshot is running");
            client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request(new String[]{dataStream})).actionGet();
            fail("Expected deleting index to fail during snapshot");
        } catch (SnapshotInProgressException e) {
            assertThat(e.getMessage(), containsString("Cannot delete data streams that are being snapshotted: ["+dataStream));
        } finally {
            logger.info("--> unblock all data nodes");
            unblockAllDataNodes("test-repo");
        }
        logger.info("--> waiting for snapshot to finish");
        CreateSnapshotResponse createSnapshotResponse = future.get();

        logger.info("Snapshot successfully completed");
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo((SnapshotState.SUCCESS)));
        assertThat(snapshotInfo.dataStreams(), contains(dataStream));
        assertThat(snapshotInfo.indices(), contains(DataStream.getDefaultBackingIndexName(dataStream, 1)));
    }
}
