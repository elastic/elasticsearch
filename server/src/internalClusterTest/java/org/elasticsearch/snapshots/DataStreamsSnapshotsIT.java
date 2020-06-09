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

package org.elasticsearch.snapshots;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.DataStreamIT;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteTransportException;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DataStreamsSnapshotsIT extends AbstractSnapshotIntegTestCase {

    public void testSnapshotAndRestore() throws Exception {
        Client client = client();

        Path location = randomRepoPath();
        createRepository("repo", "fs", location);

        DataStreamIT.createIndexTemplate("t1", "@timestamp", "ds", "other-ds");

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        request = new CreateDataStreamAction.Request("other-ds");
        response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        Map<String, Integer> source = Collections.singletonMap("@timestamp", 123);
        IndexResponse indexResponse = client.prepareIndex("ds")
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(source)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        GetSnapshotsResponse snapshot = client.admin().cluster().prepareGetSnapshots("repo").setSnapshots("snap").get();
        List<SnapshotInfo> snap = snapshot.getSnapshots("repo");
        assertEquals(1, snap.size());
        assertEquals(Collections.singletonList(".ds-ds-000001"), snap.get(0).indices());

        assertTrue(client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request("ds")).get().isAcknowledged());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        GetResponse getResponse = client.prepareGet(".ds-ds-000001", indexResponse.getId()).get();
        assertEquals(source, getResponse.getSourceAsMap());

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getIndices().size());
        assertEquals(".ds-ds-000001", ds.getDataStreams().get(0).getIndices().get(0).getName());
        assertEquals(source, client.prepareSearch("ds").get().getHits().getHits()[0].getSourceAsMap());

        restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds2")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getIndices().size());
        assertEquals(".ds-ds2-000001", ds.getDataStreams().get(0).getIndices().get(0).getName());
        assertEquals(source, client.prepareSearch("ds2").get().getHits().getHits()[0].getSourceAsMap());
    }

    public void testWildcards() throws Exception {
        Client client = client();

        Path location = randomRepoPath();
        createRepository("repo", "fs", location);

        DataStreamIT.createIndexTemplate("t1", "@timestamp", "ds", "other-ds");

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices("d*")
            .setRenamePattern("ds")
            .setRenameReplacement("ds2")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Response ds = client.admin().indices().getDataStreams(new GetDataStreamAction.Request("ds2")).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getIndices().size());
        assertEquals(".ds-ds2-000001", ds.getDataStreams().get(0).getIndices().get(0).getName());
    }

    public void testDataStreamNotStoredWhenIndexRequested() throws Exception {
        Client client = client();

        Path location = randomRepoPath();
        createRepository("repo", "fs", location);

        DataStreamIT.createIndexTemplate("t1", "@timestamp", "ds", "other-ds");

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices(".ds-ds-000001")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        expectThrows(Exception.class, () -> client.admin().cluster()
            .prepareRestoreSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get());
    }

    public void testDataStreamNotRestoredWhenIndexRequested() throws Exception {
        Client client = client();

        Path location = randomRepoPath();
        createRepository("repo", "fs", location);

        DataStreamIT.createIndexTemplate("t1", "@timestamp", "ds", "other-ds");

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("ds");
        AcknowledgedResponse response = client.admin().indices().createDataStream(request).get();
        assertTrue(response.isAcknowledged());

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
            .prepareCreateSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        assertTrue(client.admin().indices().deleteDataStream(new DeleteDataStreamAction.Request("ds")).get().isAcknowledged());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster()
            .prepareRestoreSnapshot("repo", "snap2")
            .setWaitForCompletion(true)
            .setIndices(".ds-ds-*")
            .get();

        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Request getRequest = new GetDataStreamAction.Request("ds");
        Throwable e = expectThrows(ExecutionException.class, () -> client.admin().indices().getDataStreams(getRequest).get()).getCause();
        if (e instanceof RemoteTransportException) {
            e = e.getCause();
        }
        assertEquals(ResourceNotFoundException.class, e.getClass());
    }
}
