/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, DataStreamsPlugin.class);
    }

    @Before
    public void setup() throws Exception {
        client = client();
        Path location = randomRepoPath();
        createRepository(REPO, "fs", location);

        DataStreamIT.putComposableIndexTemplate("t1", List.of("ds", "other-ds"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.execute(CreateDataStreamAction.INSTANCE, request).get();
        assertTrue(response.isAcknowledged());

        request = new CreateDataStreamAction.Request("other-ds");
        response = client.execute(CreateDataStreamAction.INSTANCE, request).get();
        assertTrue(response.isAcknowledged());

        IndexResponse indexResponse = client.prepareIndex("ds").setOpType(DocWriteRequest.OpType.CREATE).setSource(DOCUMENT_SOURCE).get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        id = indexResponse.getId();
    }

    @After
    public void cleanup() {
        AcknowledgedResponse response = client().execute(
            DeleteDataStreamAction.INSTANCE,
            new DeleteDataStreamAction.Request(new String[] { "*" })
        ).actionGet();
        assertAcked(response);
    }

    public void testSnapshotAndRestore() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
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

        assertTrue(
            client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" }))
                .get()
                .isAcknowledged()
        );

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS_BACKING_INDEX_NAME, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "ds" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
    }

    public void testSnapshotAndRestoreAll() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
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

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS_BACKING_INDEX_NAME, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "ds" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());

        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" })).get());
    }

    public void testRename() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(
            SnapshotRestoreException.class,
            () -> client.admin().cluster().prepareRestoreSnapshot(REPO, SNAPSHOT).setWaitForCompletion(true).setIndices("ds").get()
        );

        client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "ds2" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS2_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertEquals(DOCUMENT_SOURCE, client.prepareSearch("ds2").get().getHits().getHits()[0].getSourceAsMap());
        assertEquals(DOCUMENT_SOURCE, client.prepareGet(DS2_BACKING_INDEX_NAME, id).get().getSourceAsMap());
    }

    public void testBackingIndexIsNotRenamedWhenRestoringDataStream() {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(
            SnapshotRestoreException.class,
            () -> client.admin().cluster().prepareRestoreSnapshot(REPO, SNAPSHOT).setWaitForCompletion(true).setIndices("ds").get()
        );

        // delete data stream
        client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" })).actionGet();

        // restore data stream attempting to rename the backing index
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern(DS_BACKING_INDEX_NAME)
            .setRenameReplacement("new_index_name")
            .get();

        assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));

        GetDataStreamAction.Request getDSRequest = new GetDataStreamAction.Request(new String[] { "ds" });
        GetDataStreamAction.Response response = client.execute(GetDataStreamAction.INSTANCE, getDSRequest).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(DS_BACKING_INDEX_NAME));
    }

    public void testDataStreamAndBackingIndidcesAreRenamedUsingRegex() {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        expectThrows(
            SnapshotRestoreException.class,
            () -> client.admin().cluster().prepareRestoreSnapshot(REPO, SNAPSHOT).setWaitForCompletion(true).setIndices("ds").get()
        );

        // restore data stream attempting to rename the backing index
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern("(.+)")
            .setRenameReplacement("test-$1")
            .get();

        assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));

        // assert "ds" was restored as "test-ds" and the backing index has a valid name
        GetDataStreamAction.Request getRenamedDS = new GetDataStreamAction.Request(new String[] { "test-ds" });
        GetDataStreamAction.Response response = client.execute(GetDataStreamAction.INSTANCE, getRenamedDS).actionGet();
        assertThat(
            response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(),
            is(DataStream.getDefaultBackingIndexName("test-ds", 1L))
        );

        // data stream "ds" should still exist in the system
        GetDataStreamAction.Request getDSRequest = new GetDataStreamAction.Request(new String[] { "ds" });
        response = client.execute(GetDataStreamAction.INSTANCE, getDSRequest).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(DS_BACKING_INDEX_NAME));
    }

    public void testWildcards() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "ds2" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(DS2_BACKING_INDEX_NAME, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertThat(
            "we renamed the restored data stream to one that doesn't match any existing composable template",
            ds.getDataStreams().get(0).getIndexTemplate(),
            is(nullValue())
        );
    }

    public void testDataStreamNotStoredWhenIndexRequested() {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices(DS_BACKING_INDEX_NAME)
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        expectThrows(
            Exception.class,
            () -> client.admin().cluster().prepareRestoreSnapshot(REPO, "snap2").setWaitForCompletion(true).setIndices("ds").get()
        );
    }

    public void testDataStreamNotRestoredWhenIndexRequested() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        assertTrue(
            client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" }))
                .get()
                .isAcknowledged()
        );

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, "snap2")
            .setWaitForCompletion(true)
            .setIndices(".ds-ds-*")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Request getRequest = new GetDataStreamAction.Request(new String[] { "ds" });
        expectThrows(ResourceNotFoundException.class, () -> client.execute(GetDataStreamAction.INSTANCE, getRequest).actionGet());
    }

    public void testDataStreamNotIncludedInLimitedSnapshot() throws ExecutionException, InterruptedException {
        final String snapshotName = "test-snap";
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("does-not-exist-*")
            .setIncludeGlobalState(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), Matchers.is(SnapshotState.SUCCESS));

        assertThat(
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" }))
                .get()
                .isAcknowledged(),
            is(true)
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot(REPO, snapshotName).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), empty());
    }

}
