/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamsSnapshotsIT extends AbstractSnapshotIntegTestCase {

    private static final Map<String, Integer> DOCUMENT_SOURCE = Collections.singletonMap("@timestamp", 123);
    public static final String REPO = "repo";
    public static final String SNAPSHOT = "snap";
    private Client client;

    private String dsBackingIndexName;
    private String otherDsBackingIndexName;
    private String ds2BackingIndexName;
    private String otherDs2BackingIndexName;
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

        // Resolve backing index names after data streams have been created:
        // (these names have a date component, and running around midnight could lead to test failures otherwise)
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamResponse = client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        dsBackingIndexName = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().get(0).getName();
        otherDsBackingIndexName = getDataStreamResponse.getDataStreams().get(1).getDataStream().getIndices().get(0).getName();
        // Will be used in some tests, to test renaming while restoring a snapshot:
        ds2BackingIndexName = dsBackingIndexName.replace("-ds-", "-ds2-");
        otherDs2BackingIndexName = otherDsBackingIndexName.replace("-other-ds-", "-other-ds2-");

        IndexResponse indexResponse = client.prepareIndex("ds").setOpType(DocWriteRequest.OpType.CREATE).setSource(DOCUMENT_SOURCE).get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        id = indexResponse.getId();

        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        aliasesRequest.addAliasAction(
            new AliasActions(AliasActions.Type.ADD).alias("my-alias").index("ds").filter(QueryBuilders.matchAllQuery())
        );
        aliasesRequest.addAliasAction(
            new AliasActions(AliasActions.Type.ADD).alias("my-alias")
                .index("other-ds")
                .filter(QueryBuilders.matchAllQuery())
                .writeIndex(true)
        );
        assertAcked(client.admin().indices().aliases(aliasesRequest).actionGet());
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

        assertEquals(Collections.singletonList(dsBackingIndexName), getSnapshot(REPO, SNAPSHOT).indices());

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

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "ds" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(dsBackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), containsInAnyOrder("ds", "other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getName(), equalTo("my-alias"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
    }

    public void testSnapshotAndRestoreAllDataStreamsInPlace() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        assertEquals(Collections.singletonList(dsBackingIndexName), getSnapshot(REPO, SNAPSHOT).indices());

        // Close all indices:
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("*");
        closeIndexRequest.indicesOptions(IndicesOptions.strictExpandHidden());
        assertAcked(client.admin().indices().close(closeIndexRequest).actionGet());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get();
        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Request getDataSteamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response ds = client.execute(GetDataStreamAction.INSTANCE, getDataSteamRequest).get();
        assertThat(
            ds.getDataStreams().stream().map(e -> e.getDataStream().getName()).collect(Collectors.toList()),
            contains(equalTo("ds"), equalTo("other-ds"))
        );
        List<Index> backingIndices = ds.getDataStreams().get(0).getDataStream().getIndices();
        assertThat(backingIndices.stream().map(Index::getName).collect(Collectors.toList()), contains(dsBackingIndexName));
        backingIndices = ds.getDataStreams().get(1).getDataStream().getIndices();
        assertThat(backingIndices.stream().map(Index::getName).collect(Collectors.toList()), contains(otherDsBackingIndexName));
    }

    public void testSnapshotAndRestoreInPlace() {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        assertEquals(Collections.singletonList(dsBackingIndexName), getSnapshot(REPO, SNAPSHOT).indices());

        // A rollover after taking snapshot. The new backing index should be a standalone index after restoring
        // and not part of the data stream:
        RolloverRequest rolloverRequest = new RolloverRequest("ds", null);
        RolloverResponse rolloverResponse = client.admin().indices().rolloverIndex(rolloverRequest).actionGet();
        assertThat(rolloverResponse.isRolledOver(), is(true));
        String backingIndexAfterSnapshot = DataStream.getDefaultBackingIndexName("ds", 2);
        assertThat(rolloverResponse.getNewIndex(), equalTo(backingIndexAfterSnapshot));

        // Close all backing indices of ds data stream:
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(".ds-ds-*");
        closeIndexRequest.indicesOptions(IndicesOptions.strictExpandHidden());
        assertAcked(client.admin().indices().close(closeIndexRequest).actionGet());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get();
        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Request getDataSteamRequest = new GetDataStreamAction.Request(new String[] { "ds" });
        GetDataStreamAction.Response ds = client.execute(GetDataStreamAction.INSTANCE, getDataSteamRequest).actionGet();
        assertThat(
            ds.getDataStreams().stream().map(e -> e.getDataStream().getName()).collect(Collectors.toList()),
            contains(equalTo("ds"))
        );
        List<Index> backingIndices = ds.getDataStreams().get(0).getDataStream().getIndices();
        assertThat(ds.getDataStreams().get(0).getDataStream().getIndices(), hasSize(1));
        assertThat(backingIndices.stream().map(Index::getName).collect(Collectors.toList()), contains(equalTo(dsBackingIndexName)));

        // The backing index created as part of rollover should still exist (but just not part of the data stream)
        assertThat(indexExists(backingIndexAfterSnapshot), is(true));
        // An additional rollover should create a new backing index (3th generation) and leave .ds-ds-...-2 index as is:
        rolloverRequest = new RolloverRequest("ds", null);
        rolloverResponse = client.admin().indices().rolloverIndex(rolloverRequest).actionGet();
        assertThat(rolloverResponse.isRolledOver(), is(true));
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("ds", 3)));
    }

    public void testSnapshotAndRestoreAllIncludeSpecificDataStream() throws Exception {
        IndexResponse indexResponse = client.prepareIndex("other-ds")
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource(DOCUMENT_SOURCE)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        String id2 = indexResponse.getId();

        String idToGet;
        String dataStreamToSnapshot;
        String backingIndexName;
        if (randomBoolean()) {
            dataStreamToSnapshot = "ds";
            idToGet = this.id;
            backingIndexName = this.dsBackingIndexName;
        } else {
            dataStreamToSnapshot = "other-ds";
            idToGet = id2;
            backingIndexName = this.otherDsBackingIndexName;
        }
        boolean filterDuringSnapshotting = randomBoolean();

        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest(REPO, SNAPSHOT);
        createSnapshotRequest.waitForCompletion(true);
        if (filterDuringSnapshotting) {
            createSnapshotRequest.indices(dataStreamToSnapshot);
        }
        createSnapshotRequest.includeGlobalState(false);
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().createSnapshot(createSnapshotRequest).actionGet();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        if (filterDuringSnapshotting) {
            assertThat(getSnapshot(REPO, SNAPSHOT).indices(), containsInAnyOrder(backingIndexName));
        } else {
            assertThat(getSnapshot(REPO, SNAPSHOT).indices(), containsInAnyOrder(dsBackingIndexName, otherDsBackingIndexName));
        }

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(REPO, SNAPSHOT);
        restoreSnapshotRequest.waitForCompletion(true);
        restoreSnapshotRequest.includeGlobalState(false);
        if (filterDuringSnapshotting == false) {
            restoreSnapshotRequest.indices(dataStreamToSnapshot);
        }
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().restoreSnapshot(restoreSnapshotRequest).actionGet();

        assertEquals(1, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(backingIndexName, idToGet).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch(backingIndexName).get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { dataStreamToSnapshot })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(backingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), contains(dataStreamToSnapshot));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get(dataStreamToSnapshot),
            equalTo(
                List.of(
                    new DataStreamAlias(
                        "my-alias",
                        List.of(dataStreamToSnapshot),
                        "other-ds".equals(dataStreamToSnapshot) ? "other-ds" : null,
                        Map.of("other-ds", Map.of("match_all", Map.of("boost", 1f)), "ds", Map.of("match_all", Map.of("boost", 1f)))
                    )
                )
            )
        );

        DeleteDataStreamAction.Request r = new DeleteDataStreamAction.Request(new String[] { dataStreamToSnapshot });
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, r).get());
    }

    public void testSnapshotAndRestoreReplaceAll() throws Exception {
        var createSnapshotRequest = new CreateSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true).includeGlobalState(false);
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().createSnapshot(createSnapshotRequest).actionGet();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        assertThat(getSnapshot(REPO, SNAPSHOT).indices(), containsInAnyOrder(dsBackingIndexName, otherDsBackingIndexName));

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        var restoreSnapshotRequest = new RestoreSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true).includeGlobalState(false);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().restoreSnapshot(restoreSnapshotRequest).actionGet();

        assertEquals(2, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "*" })
        ).get();
        assertEquals(2, ds.getDataStreams().size());
        assertThat(
            ds.getDataStreams().stream().map(i -> i.getDataStream().getName()).collect(Collectors.toList()),
            containsInAnyOrder("ds", "other-ds")
        );

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), containsInAnyOrder("ds", "other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getDataStreams(), containsInAnyOrder("ds", "other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getDataStreams(), containsInAnyOrder("ds", "other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );

        DeleteDataStreamAction.Request r = new DeleteDataStreamAction.Request(new String[] { "*" });
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, r).get());
    }

    public void testSnapshotAndRestoreAll() throws Exception {
        var createSnapshotRequest = new CreateSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true).includeGlobalState(false);
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().createSnapshot(createSnapshotRequest).actionGet();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        assertThat(getSnapshot(REPO, SNAPSHOT).indices(), containsInAnyOrder(dsBackingIndexName, otherDsBackingIndexName));

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        var restoreSnapshotRequest = new RestoreSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true).includeGlobalState(false);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().restoreSnapshot(restoreSnapshotRequest).actionGet();
        assertEquals(2, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "*" })
        ).get();
        assertEquals(2, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(dsBackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertEquals(1, ds.getDataStreams().get(1).getDataStream().getIndices().size());
        assertEquals(otherDsBackingIndexName, ds.getDataStreams().get(1).getDataStream().getIndices().get(0).getName());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), containsInAnyOrder("ds", "other-ds"));

        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getDataStreams(), containsInAnyOrder("ds", "other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );

        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getDataStreams(), containsInAnyOrder("ds", "other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );

        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" })).get());
    }

    public void testSnapshotAndRestoreIncludeAliasesFalse() throws Exception {
        var createSnapshotRequest = new CreateSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true).includeGlobalState(false);
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().createSnapshot(createSnapshotRequest).actionGet();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);
        assertThat(getSnapshot(REPO, SNAPSHOT).indices(), containsInAnyOrder(dsBackingIndexName, otherDsBackingIndexName));

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN));

        var restoreSnapshotRequest = new RestoreSnapshotRequest(REPO, SNAPSHOT).waitForCompletion(true)
            .includeGlobalState(false)
            .includeAliases(false);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin().cluster().restoreSnapshot(restoreSnapshotRequest).actionGet();
        assertEquals(2, restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertEquals(DOCUMENT_SOURCE, client.prepareGet(dsBackingIndexName, id).get().getSourceAsMap());
        SearchHit[] hits = client.prepareSearch("ds").get().getHits().getHits();
        assertEquals(1, hits.length);
        assertEquals(DOCUMENT_SOURCE, hits[0].getSourceAsMap());

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "*" })
        ).get();
        assertEquals(2, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(dsBackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertEquals(1, ds.getDataStreams().get(1).getDataStream().getIndices().size());
        assertEquals(otherDsBackingIndexName, ds.getDataStreams().get(1).getDataStream().getIndices().get(0).getName());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("*")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases(), anEmptyMap());
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
        assertEquals(ds2BackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
        assertEquals(DOCUMENT_SOURCE, client.prepareSearch("ds2").get().getHits().getHits()[0].getSourceAsMap());
        assertEquals(DOCUMENT_SOURCE, client.prepareGet(ds2BackingIndexName, id).get().getSourceAsMap());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), containsInAnyOrder("ds", "ds2", "other-ds"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds2").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds2").get(0).getName(), equalTo("my-alias"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds2").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getWriteDataStream(), equalTo("other-ds"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
    }

    public void testRenameWriteDataStream() throws Exception {
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("other-ds")
            .setIncludeGlobalState(false)
            .get();

        RestStatus status = createSnapshotResponse.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("other-ds")
            .setRenamePattern("other-ds")
            .setRenameReplacement("other-ds2")
            .get();

        GetDataStreamAction.Response ds = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "other-ds2" })
        ).get();
        assertEquals(1, ds.getDataStreams().size());
        assertEquals(1, ds.getDataStreams().get(0).getDataStream().getIndices().size());
        assertEquals(otherDs2BackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());

        GetAliasesResponse getAliasesResponse = client.admin().indices().getAliases(new GetAliasesRequest("my-alias")).actionGet();
        assertThat(getAliasesResponse.getDataStreamAliases().keySet(), containsInAnyOrder("ds", "other-ds", "other-ds2"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds2").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds2").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds2").get(0).getWriteDataStream(), equalTo("other-ds2"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds2").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("ds").get(0).getWriteDataStream(), equalTo("other-ds2"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").size(), equalTo(1));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getName(), equalTo("my-alias"));
        assertThat(getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getWriteDataStream(), equalTo("other-ds2"));
        assertThat(
            getAliasesResponse.getDataStreamAliases().get("other-ds").get(0).getFilter("ds").string(),
            equalTo("{\"match_all\":{\"boost\":1.0}}")
        );
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
            .setRenamePattern(dsBackingIndexName)
            .setRenameReplacement("new_index_name")
            .get();

        assertThat(restoreSnapshotResponse.status(), is(RestStatus.OK));

        GetDataStreamAction.Request getDSRequest = new GetDataStreamAction.Request(new String[] { "ds" });
        GetDataStreamAction.Response response = client.execute(GetDataStreamAction.INSTANCE, getDSRequest).actionGet();
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(dsBackingIndexName));
    }

    public void testDataStreamAndBackingIndicesAreRenamedUsingRegex() {
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
        assertThat(response.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), is(dsBackingIndexName));
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
        assertEquals(ds2BackingIndexName, ds.getDataStreams().get(0).getDataStream().getIndices().get(0).getName());
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
            .setIndices(dsBackingIndexName)
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

    public void testDeleteDataStreamDuringSnapshot() throws Exception {
        Client client1 = client();

        // this test uses a MockRepository
        assertAcked(client().admin().cluster().prepareDeleteRepository(REPO));

        final String repositoryName = "test-repo";
        createRepository(
            repositoryName,
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put("block_on_data", true)
        );

        String dataStream = "datastream";
        DataStreamIT.putComposableIndexTemplate("dst", List.of(dataStream));

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client1.prepareIndex(dataStream)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setId(Integer.toString(i))
                .setSource(Collections.singletonMap("@timestamp", "2020-12-12"))
                .execute()
                .actionGet();
        }
        refresh();
        assertDocCount(dataStream, 100L);
        // Resolve backing index name after the data stream has been created because it has a date component,
        // and running around midnight could lead to test failures otherwise
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStream });
        GetDataStreamAction.Response getDataStreamResponse = client.execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        String backingIndexName = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().get(0).getName();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> future = client1.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, SNAPSHOT)
            .setIndices(dataStream)
            .setWaitForCompletion(true)
            .setPartial(false)
            .execute();
        logger.info("--> wait for block to kick in");
        waitForBlockOnAnyDataNode(repositoryName);

        // non-partial snapshots do not allow delete operations on data streams where snapshot has not been completed
        try {
            logger.info("--> delete index while non-partial snapshot is running");
            client1.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { dataStream })).actionGet();
            fail("Expected deleting index to fail during snapshot");
        } catch (SnapshotInProgressException e) {
            assertThat(e.getMessage(), containsString("Cannot delete data streams that are being snapshotted: [" + dataStream));
        } finally {
            logger.info("--> unblock all data nodes");
            unblockAllDataNodes(repositoryName);
        }
        logger.info("--> waiting for snapshot to finish");
        CreateSnapshotResponse createSnapshotResponse = future.get();

        logger.info("--> snapshot successfully completed");
        SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo((SnapshotState.SUCCESS)));
        assertThat(snapshotInfo.dataStreams(), contains(dataStream));
        assertThat(snapshotInfo.indices(), contains(backingIndexName));
    }

    public void testCloneSnapshotThatIncludesDataStream() throws Exception {
        final String sourceSnapshotName = "snap-source";
        final String indexWithoutDataStream = "test-idx-no-ds";
        createIndexWithContent(indexWithoutDataStream);
        assertSuccessful(
            client.admin()
                .cluster()
                .prepareCreateSnapshot(REPO, sourceSnapshotName)
                .setWaitForCompletion(true)
                .setIndices("ds", indexWithoutDataStream)
                .setIncludeGlobalState(false)
                .execute()
        );
        assertAcked(
            client().admin()
                .cluster()
                .prepareCloneSnapshot(REPO, sourceSnapshotName, "target-snapshot-1")
                .setIndices(indexWithoutDataStream)
                .get()
        );
    }

    public void testPartialRestoreSnapshotThatIncludesDataStream() {
        final String snapshot = "test-snapshot";
        final String indexWithoutDataStream = "test-idx-no-ds";
        createIndexWithContent(indexWithoutDataStream);
        createFullSnapshot(REPO, snapshot);
        assertAcked(client.admin().indices().prepareDelete(indexWithoutDataStream));
        RestoreInfo restoreInfo = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, snapshot)
            .setIndices(indexWithoutDataStream)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(randomBoolean())
            .get()
            .getRestoreInfo();
        assertThat(restoreInfo.failedShards(), is(0));
        assertThat(restoreInfo.successfulShards(), is(1));
    }

    public void testSnapshotDSDuringRollover() throws Exception {
        // repository consistency check requires at least one snapshot per registered repository
        createFullSnapshot(REPO, "snap-so-repo-checks-pass");
        final String repoName = "mock-repo";
        createRepository(repoName, "mock");
        final boolean partial = randomBoolean();
        blockAllDataNodes(repoName);
        final String snapshotName = "ds-snap";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setPartial(partial)
            .setIncludeGlobalState(randomBoolean())
            .execute();
        waitForBlockOnAnyDataNode(repoName);
        awaitNumberOfSnapshotsInProgress(1);
        final ActionFuture<RolloverResponse> rolloverResponse = indicesAdmin().rolloverIndex(new RolloverRequest("ds", null));

        if (partial) {
            assertTrue(rolloverResponse.get().isRolledOver());
        } else {
            SnapshotInProgressException e = expectThrows(SnapshotInProgressException.class, rolloverResponse::actionGet);
            assertThat(e.getMessage(), containsString("Cannot roll over data stream that is being snapshotted:"));
        }
        unblockAllDataNodes(repoName);
        final SnapshotInfo snapshotInfo = assertSuccessful(snapshotFuture);

        assertThat(snapshotInfo.dataStreams(), hasItems("ds"));
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "ds" })).get());

        RestoreInfo restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("ds")
            .get()
            .getRestoreInfo();

        assertEquals(restoreSnapshotResponse.successfulShards(), restoreSnapshotResponse.totalShards());
        assertEquals(restoreSnapshotResponse.failedShards(), 0);
    }

    public void testSnapshotDSDuringRolloverAndDeleteOldIndex() throws Exception {
        // repository consistency check requires at least one snapshot per registered repository
        createFullSnapshot(REPO, "snap-so-repo-checks-pass");
        final String repoName = "mock-repo";
        createRepository(repoName, "mock");
        blockAllDataNodes(repoName);
        final String snapshotName = "ds-snap";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setPartial(true)
            .setIncludeGlobalState(randomBoolean())
            .execute();
        waitForBlockOnAnyDataNode(repoName);
        awaitNumberOfSnapshotsInProgress(1);
        final RolloverResponse rolloverResponse = indicesAdmin().rolloverIndex(new RolloverRequest("ds", null)).get();
        assertTrue(rolloverResponse.isRolledOver());

        logger.info("--> deleting former write index");
        assertAcked(indicesAdmin().prepareDelete(rolloverResponse.getOldIndex()));

        unblockAllDataNodes(repoName);
        final SnapshotInfo snapshotInfo = assertSuccessful(snapshotFuture);

        assertThat(
            "snapshot should not contain 'ds' since none of its indices existed both at the start and at the end of the snapshot",
            snapshotInfo.dataStreams(),
            not(hasItems("ds"))
        );
        assertAcked(
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "other-ds" })).get()
        );

        RestoreInfo restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("other-ds")
            .get()
            .getRestoreInfo();

        assertEquals(restoreSnapshotResponse.successfulShards(), restoreSnapshotResponse.totalShards());
        assertEquals(restoreSnapshotResponse.failedShards(), 0);
    }

    public void testExcludeDSFromSnapshotWhenExcludingItsIndices() {
        final String snapshot = "test-snapshot";
        final String indexWithoutDataStream = "test-idx-no-ds";
        createIndexWithContent(indexWithoutDataStream);
        final SnapshotInfo snapshotInfo = createSnapshot(REPO, snapshot, List.of("*", "-.*"));
        assertThat(snapshotInfo.dataStreams(), empty());
        assertAcked(client.admin().indices().prepareDelete(indexWithoutDataStream));
        RestoreInfo restoreInfo = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, snapshot)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(randomBoolean())
            .get()
            .getRestoreInfo();
        assertThat(restoreInfo.failedShards(), is(0));
        assertThat(restoreInfo.successfulShards(), is(1));
    }

    public void testRestoreSnapshotFully() throws Exception {
        final String snapshotName = "test-snapshot";
        final String indexName = "test-idx";
        createIndexWithContent(indexName);
        createFullSnapshot(REPO, snapshotName);

        assertAcked(client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { "*" })).get());
        assertAcked(client.admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.lenientExpandOpenHidden()).get());

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, snapshotName)
            .setWaitForCompletion(true)
            .get();
        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        GetDataStreamAction.Request getRequest = new GetDataStreamAction.Request(new String[] { "*" });
        assertThat(client.execute(GetDataStreamAction.INSTANCE, getRequest).get().getDataStreams(), hasSize(2));
        assertNotNull(client.admin().indices().prepareGetIndex().setIndices(indexName).get());
    }

    public void testRestoreDataStreamAliasWithConflictingDataStream() throws Exception {
        var snapshotName = "test-snapshot";
        createFullSnapshot(REPO, snapshotName);
        client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet();
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("my-*"));
        try {
            var request = new CreateDataStreamAction.Request("my-alias");
            assertAcked(client.execute(CreateDataStreamAction.INSTANCE, request).actionGet());
            var e = expectThrows(
                IllegalStateException.class,
                () -> client.admin().cluster().prepareRestoreSnapshot(REPO, snapshotName).setWaitForCompletion(true).get()
            );
            assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));
        } finally {
            // Need to remove data streams in order to remove template
            client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet();
            // Need to remove template, because base class doesn't remove composable index templates after each test (only legacy templates)
            client.execute(DeleteComposableIndexTemplateAction.INSTANCE, new DeleteComposableIndexTemplateAction.Request("my-template"))
                .actionGet();
        }
    }

    public void testRestoreDataStreamAliasWithConflictingIndicesAlias() throws Exception {
        var snapshotName = "test-snapshot";
        createFullSnapshot(REPO, snapshotName);
        client.execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index").alias(new Alias("my-alias"));
        assertAcked(client.admin().indices().create(createIndexRequest).actionGet());

        var e = expectThrows(
            IllegalStateException.class,
            () -> client.admin().cluster().prepareRestoreSnapshot(REPO, snapshotName).setWaitForCompletion(true).get()
        );
        assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (my-alias)"));
    }
}
