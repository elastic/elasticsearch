/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end coverage for slice-enabled indices, where {@code _id} is stored as a composite {@code slice#id}
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class SliceCompositeIdIT extends ESIntegTestCase {

    @Before
    public void requireSliceFeatureFlag() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // InternalSettingsPlugin registers the otherwise-unregistered index.recovery.file_based_threshold test setting.
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), InternalSettingsPlugin.class);
    }

    private void createSliceIndex(String index, int replicas) {
        createSliceIndex(index, replicas, Settings.EMPTY);
    }

    private void createSliceIndex(String index, int replicas, Settings extraSettings) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", replicas)
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                    .put(extraSettings)
            ).setMapping("field", "type=keyword")
        );
        ensureGreen(index);
    }

    private DocWriteResponse indexDoc(String index, String slice, String id, String value) {
        IndexRequest request = new IndexRequest(index).source("field", value).routing(slice).setRoutingFromSlice(true);
        if (id != null) {
            request.id(id);
        }
        return client().index(request).actionGet();
    }

    private GetResponse getDoc(String index, String slice, String id) {
        return client().get(new GetRequest(index, id).routing(slice).setRoutingFromSlice(true)).actionGet();
    }

    /** Randomly refresh so the following read exercises either the live version map (no refresh) or the Lucene index. */
    private void maybeRefresh(String index) {
        if (randomBoolean()) {
            refresh(index);
        }
    }

    private DocWriteResponse deleteDoc(String index, String slice, String id) {
        return client().delete(new DeleteRequest(index, id).routing(slice).setRoutingFromSlice(true)).actionGet();
    }

    private SearchRequestBuilder searchSlice(String index, String slice, QueryBuilder query) {
        // A slice-enabled index surfaces _slice by default (and never _routing), so no explicit fetch-field is needed.
        SearchRequestBuilder search = prepareSearch(index).setQuery(query);
        search.request().searchSlice(slice);
        return search;
    }

    /**
     * The same {@code _id} indexed under two slices yields two distinct documents on the same shard (both CREATED), each
     * independently retrievable by GET, search and ids-query within its slice, with the plain user id surfaced everywhere.
     * Deleting one slice's document leaves the other intact.
     */
    public void testSameIdAcrossSlicesAreDistinctDocuments() {
        createSliceIndex("idx", 1);

        DocWriteResponse a = indexDoc("idx", "sa", "1", "va");
        DocWriteResponse b = indexDoc("idx", "sb", "1", "vb");
        // Both are new documents (the composite _id keeps them distinct on the single shard), and the returned id is plain.
        assertThat(a.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(b.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(a.getId(), equalTo("1"));
        assertThat(b.getId(), equalTo("1"));
        refresh("idx");

        // GET within each slice returns that slice's document, with the plain id, exposing _slice and never _routing.
        GetResponse ga = getDoc("idx", "sa", "1");
        assertThat(ga.isExists(), equalTo(true));
        assertThat(ga.getId(), equalTo("1"));
        assertThat(ga.getSource().get("field"), equalTo("va"));
        assertThat(ga.getField("_routing"), equalTo(null));
        assertThat(ga.getField(SliceIndexing.FIELD_NAME).getValue(), equalTo("sa"));
        GetResponse gb = getDoc("idx", "sb", "1");
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));

        // A match-all search scoped to a slice sees only that slice's document; _all sees both.
        assertResponse(searchSlice("idx", "sa", QueryBuilders.matchAllQuery()), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            SearchHit hit = r.getHits().getAt(0);
            assertThat(hit.getId(), equalTo("1"));
            assertThat(hit.getSourceAsMap().get("field"), equalTo("va"));
            // The hit surfaces the slice as _slice and never leaks it as _routing.
            assertThat(hit.field(SliceIndexing.FIELD_NAME).getValue(), equalTo("sa"));
            assertThat(hit.field("_routing"), equalTo(null));
        });
        // _all sees both, and each hit carries its own _slice so same-id docs are distinguishable.
        assertResponse(searchSlice("idx", SliceIndexing.SLICE_ALL, QueryBuilders.matchAllQuery()), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(2L));
            Map<String, String> sliceByValue = new HashMap<>();
            for (SearchHit hit : r.getHits().getHits()) {
                assertThat(hit.getId(), equalTo("1"));
                assertThat(hit.field("_routing"), equalTo(null));
                sliceByValue.put((String) hit.getSourceAsMap().get("field"), hit.field(SliceIndexing.FIELD_NAME).getValue());
            }
            assertThat(sliceByValue, equalTo(Map.of("va", "sa", "vb", "sb")));
        });

        // An ids query is scoped by slice too: id "1" in slice sa resolves to the sa document only.
        assertResponse(searchSlice("idx", "sa", QueryBuilders.idsQuery().addIds("1")), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(r.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(r.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("va"));
        });
        assertResponse(searchSlice("idx", "sb", QueryBuilders.idsQuery().addIds("1")), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(r.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("vb"));
        });

        // Deleting id "1" in slice sa must not touch id "1" in slice sb.
        assertThat(deleteDoc("idx", "sa", "1").getResult(), equalTo(DocWriteResponse.Result.DELETED));
        refresh("idx");
        assertThat(getDoc("idx", "sa", "1").isExists(), equalTo(false));
        assertThat(getDoc("idx", "sb", "1").isExists(), equalTo(true));
    }

    /**
     * A slice-enabled index hides {@code _routing} from retrieval: even an explicit {@code fields} request for it returns
     * nothing, while {@code _slice} is retrievable. Routing and slicing never overlap in responses.
     */
    public void testRoutingFieldIsHiddenOnSliceIndex() {
        createSliceIndex("hidden", 1);
        indexDoc("hidden", "sa", "1", "va");
        refresh("hidden");

        SearchRequestBuilder search = prepareSearch("hidden").setQuery(QueryBuilders.matchAllQuery())
            .addFetchField("_routing")
            .addFetchField(SliceIndexing.FIELD_NAME);
        search.request().searchSlice("sa");
        assertResponse(search, r -> {
            SearchHit hit = r.getHits().getAt(0);
            assertThat(hit.field("_routing"), equalTo(null));
            assertThat(hit.field(SliceIndexing.FIELD_NAME).getValue(), equalTo("sa"));
        });
    }

    /**
     * Update-by-id is scoped to the slice: updating id "1" in one slice must not affect id "1" in another slice.
     */
    public void testUpdateByIdIsScopedToSlice() {
        createSliceIndex("upd", 1);
        indexDoc("upd", "sa", "1", "va");
        indexDoc("upd", "sb", "1", "vb");
        // The update resolves the doc through either the live version map or Lucene depending on whether we refreshed.
        maybeRefresh("upd");

        DocWriteResponse updated = client().update(
            new UpdateRequest("upd", "1").doc("field", "va-updated").routing("sa").setRoutingFromSlice(true)
        ).actionGet();
        assertThat(updated.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat(updated.getId(), equalTo("1"));
        refresh("upd");

        assertThat(getDoc("upd", "sa", "1").getSource().get("field"), equalTo("va-updated"));
        // The other slice's document with the same id is untouched.
        assertThat(getDoc("upd", "sb", "1").getSource().get("field"), equalTo("vb"));
    }

    /**
     * The bulk API treats the same id in different slices as distinct documents, and supports per-slice update and delete.
     */
    public void testBulkAcrossSlices() {
        createSliceIndex("bulk", 1);

        BulkRequest indexBulk = new BulkRequest();
        indexBulk.add(new IndexRequest("bulk").id("1").source("field", "va").routing("sa").setRoutingFromSlice(true));
        indexBulk.add(new IndexRequest("bulk").id("1").source("field", "vb").routing("sb").setRoutingFromSlice(true));
        BulkResponse indexResponse = client().bulk(indexBulk).actionGet();
        assertThat(indexResponse.buildFailureMessage(), indexResponse.hasFailures(), equalTo(false));
        assertThat(indexResponse.getItems()[0].getId(), equalTo("1"));
        assertThat(indexResponse.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(indexResponse.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        maybeRefresh("bulk");

        assertThat(getDoc("bulk", "sa", "1").getSource().get("field"), equalTo("va"));
        assertThat(getDoc("bulk", "sb", "1").getSource().get("field"), equalTo("vb"));

        // Bulk update of one slice and delete of the other, both targeting id "1".
        BulkRequest mutateBulk = new BulkRequest();
        mutateBulk.add(new UpdateRequest("bulk", "1").doc("field", "va2").routing("sa").setRoutingFromSlice(true));
        mutateBulk.add(new DeleteRequest("bulk", "1").routing("sb").setRoutingFromSlice(true));
        BulkResponse mutateResponse = client().bulk(mutateBulk).actionGet();
        assertThat(mutateResponse.buildFailureMessage(), mutateResponse.hasFailures(), equalTo(false));
        maybeRefresh("bulk");

        assertThat(getDoc("bulk", "sa", "1").getSource().get("field"), equalTo("va2"));
        assertThat(getDoc("bulk", "sb", "1").isExists(), equalTo(false));
    }

    /**
     * Multi-get resolves each item against its own slice: the same {@code _id} fetched under two slices returns the two
     * distinct documents, with the plain id surfaced. An item missing {@code _slice} fails (per-item) on a slice index.
     */
    public void testMgetAcrossSlices() {
        createSliceIndex("mg", 1);
        indexDoc("mg", "sa", "1", "va");
        indexDoc("mg", "sb", "1", "vb");
        refresh("mg");

        MultiGetRequest request = new MultiGetRequest().add(new MultiGetRequest.Item("mg", "1").routing("sa").setRoutingFromSlice(true))
            .add(new MultiGetRequest.Item("mg", "1").routing("sb").setRoutingFromSlice(true));
        MultiGetItemResponse[] items = client().multiGet(request).actionGet().getResponses();
        assertThat(items.length, equalTo(2));
        assertThat(items[0].getResponse().getId(), equalTo("1"));
        assertThat(items[0].getResponse().getSource().get("field"), equalTo("va"));
        assertThat(items[1].getResponse().getId(), equalTo("1"));
        assertThat(items[1].getResponse().getSource().get("field"), equalTo("vb"));

        // An item without _slice on a slice index fails (per-item), like single GET.
        MultiGetItemResponse missing = client().multiGet(new MultiGetRequest().add(new MultiGetRequest.Item("mg", "1")))
            .actionGet()
            .getResponses()[0];
        assertThat(missing.getFailure(), not(equalTo(null)));
        assertThat(
            missing.getFailure().getFailure().getMessage(),
            containsString("[slice] is required when [index.slice.enabled] is true")
        );
    }

    /**
     * Auto-generated ids must work with slice-enabled indices, and the id surfaced back to the user (index response, GET,
     * search hit) must be the plain generated id — never the internal composite (which would contain a '#').
     */
    public void testAutoGeneratedId() {
        createSliceIndex("auto", 1);

        DocWriteResponse response = indexDoc("auto", "sa", null, "va");
        final String generatedId = response.getId();
        assertThat(generatedId, not(emptyOrNullString()));
        assertThat("the returned id must be the plain auto-generated id, not the composite", generatedId, not(containsString("#")));
        refresh("auto");

        GetResponse get = getDoc("auto", "sa", generatedId);
        assertThat(get.isExists(), equalTo(true));
        assertThat(get.getId(), equalTo(generatedId));
        assertThat(get.getSource().get("field"), equalTo("va"));

        assertResponse(searchSlice("auto", "sa", QueryBuilders.matchAllQuery()), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(r.getHits().getAt(0).getId(), equalTo(generatedId));
        });
    }

    /**
     * Slice-scoped index and delete operations must survive a full cluster restart. On a graceful restart the shard flushes
     * on close, so the recovered shard is restored from its Lucene commit rather than by replaying the translog; it must
     * still reflect that the delete targeted only its slice's composite term, leaving the same id in another slice intact.
     * (Translog replay of slice ops is covered at the unit level by SliceChangesSnapshotTests.)
     */
    public void testSliceDocsSurviveFullRestart() throws Exception {
        createSliceIndex("rec", 1);

        indexDoc("rec", "sa", "1", "va");
        indexDoc("rec", "sb", "1", "vb");
        indexDoc("rec", "sa", "2", "v2");
        // Commit the initial docs so the following delete/update land after the last Lucene commit.
        flush("rec");

        // Delete slice sa's id "1" and replace slice sa's id "2" after the commit; the full restart must preserve both.
        deleteDoc("rec", "sa", "1");
        indexDoc("rec", "sa", "2", "v2-updated");
        refresh("rec");

        internalCluster().fullRestart();
        ensureGreen("rec");

        // The delete hit only slice sa's id "1"; the same id in slice sb is untouched.
        assertThat(getDoc("rec", "sa", "1").isExists(), equalTo(false));
        GetResponse gb = getDoc("rec", "sb", "1");
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));
        // The replaced doc reflects its latest value.
        assertThat(getDoc("rec", "sa", "2").getSource().get("field"), equalTo("v2-updated"));

        assertResponse(
            searchSlice("rec", SliceIndexing.SLICE_ALL, QueryBuilders.matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(2L))
        );
    }

    /**
     * Slice-scoped index and delete ops must replicate correctly: after the primary is stopped and the replica (which
     * received the ops via replication) is promoted, the promoted copy reflects that the delete hit only its slice.
     */
    public void testSliceDocsSurviveReplicaPromotion() throws Exception {
        createSliceIndex("peer", 1); // 1 replica on the 2-node cluster
        indexDoc("peer", "sa", "1", "va");
        indexDoc("peer", "sb", "1", "vb");
        deleteDoc("peer", "sa", "1");
        refresh("peer");
        ensureGreen("peer");

        // Stop the node holding the primary so the former replica is promoted and serves the reads.
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        String primaryNodeId = state.routingTable().index("peer").shard(0).primaryShard().currentNodeId();
        internalCluster().stopNode(state.nodes().get(primaryNodeId).getName());
        ensureYellow("peer");

        assertThat(getDoc("peer", "sa", "1").isExists(), equalTo(false));
        GetResponse gb = getDoc("peer", "sb", "1");
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));
        assertResponse(
            searchSlice("peer", SliceIndexing.SLICE_ALL, QueryBuilders.matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L))
        );
    }

    private ClusterState clusterState() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
    }

    private IndexShardRoutingTable shardTable(String index) {
        return clusterState().routingTable().index(index).shard(0);
    }

    private ShardRouting primaryRouting(String index) {
        return shardTable(index).primaryShard();
    }

    private ShardRouting replicaRouting(String index) {
        return shardTable(index).replicaShards().get(0);
    }

    private String nodeName(String nodeId) {
        return clusterState().nodes().get(nodeId).getName();
    }

    private RecoveryState replicaRecoveryState(String index) {
        return indicesAdmin().prepareRecoveries(index)
            .get()
            .shardRecoveryStates()
            .get(index)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .orElseThrow();
    }

    /** GET a slice-scoped doc pinned to a specific shard copy, so a test can read the recovered/relocated copy. */
    private GetResponse getDocFromNode(String index, String slice, String id, String nodeId) {
        return client().get(new GetRequest(index, id).routing(slice).setRoutingFromSlice(true).preference("_only_nodes:" + nodeId))
            .actionGet();
    }

    /** Index the shared recovery scenario, then (after a commit) delete slice sa's id "1" and replace slice sa's id "2". */
    private void indexSliceScenario(String index) {
        indexDoc(index, "sa", "1", "va");
        indexDoc(index, "sb", "1", "vb");
        indexDoc(index, "sa", "2", "v2");
        flush(index);
        deleteDoc(index, "sa", "1");
        indexDoc(index, "sa", "2", "v2-updated");
    }

    /**
     * Asserts the slice invariant on the copy hosted by {@code nodeId}: the delete hit only slice sa's compound term
     * (sa/1 is gone), the same id in slice sb is intact, and sa/2 reflects its latest value.
     */
    private void assertRecoveredSliceState(String index, String nodeId) {
        assertThat(getDocFromNode(index, "sa", "1", nodeId).isExists(), equalTo(false));
        GetResponse gb = getDocFromNode(index, "sb", "1", nodeId);
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));
        assertThat(getDocFromNode(index, "sa", "2", nodeId).getSource().get("field"), equalTo("v2-updated"));
    }

    /**
     * Ops-based (sequence-number-based) peer recovery: after a replica's node restarts having missed some slice ops, the
     * primary replays them from its translog without copying segment files. The recovered replica must reflect that the
     * delete hit only slice sa's compound term, leaving the same id in slice sb intact.
     */
    public void testSliceOpsBasedPeerRecovery() throws Exception {
        // A 100% file-based threshold keeps recovery on the ops path as long as the peer-recovery retention lease holds.
        createSliceIndex("ops", 1, Settings.builder().put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0).build());
        indexDoc("ops", "sa", "1", "va");
        indexDoc("ops", "sb", "1", "vb");
        indexDoc("ops", "sa", "2", "v2");
        flush("ops");

        String replicaNode = nodeName(replicaRouting("ops").currentNodeId());
        // While the replica's node is down the primary accumulates the delete/replace that recovery must replay.
        internalCluster().restartNode(replicaNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                deleteDoc("ops", "sa", "1");
                indexDoc("ops", "sa", "2", "v2-updated");
                return Settings.EMPTY;
            }
        });
        ensureGreen("ops");

        RecoveryState recovery = replicaRecoveryState("ops");
        assertThat(recovery.getRecoverySource(), equalTo(RecoverySource.PeerRecoverySource.INSTANCE));
        assertThat("ops-based recovery copies no files", recovery.getIndex().totalFileCount(), equalTo(0));
        assertThat("ops-based recovery replays translog ops", recovery.getTranslog().recoveredOperations(), greaterThan(0));

        refresh("ops");
        assertRecoveredSliceState("ops", replicaRouting("ops").currentNodeId());
    }

    /**
     * File-based ("full copy") peer recovery: allocating a fresh replica copies the primary's segment files wholesale
     * (there is no local history to replay). The recovered replica must be slice-correct.
     */
    public void testSliceFileBasedPeerRecovery() throws Exception {
        createSliceIndex("file", 0);
        indexSliceScenario("file");

        setReplicaCount(1, "file");
        ensureGreen("file");

        RecoveryState recovery = replicaRecoveryState("file");
        assertThat(recovery.getRecoverySource(), equalTo(RecoverySource.PeerRecoverySource.INSTANCE));
        assertThat("a fresh replica is built by copying segment files", recovery.getIndex().totalFileCount(), greaterThan(0));

        refresh("file");
        assertRecoveredSliceState("file", replicaRouting("file").currentNodeId());
    }

    /**
     * Relocation: moving the shard to another node peer-recovers it there, replaying the post-commit slice ops. The
     * relocated copy must reflect that the delete hit only slice sa's compound term.
     */
    public void testSliceDocsSurviveRelocation() throws Exception {
        createSliceIndex("reloc", 0);
        indexSliceScenario("reloc");

        String fromNode = nodeName(primaryRouting("reloc").currentNodeId());
        String toNode = clusterState().nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .filter(name -> name.equals(fromNode) == false)
            .findFirst()
            .orElseThrow();
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("reloc", 0, fromNode, toNode));
        ensureGreen("reloc");
        assertThat(nodeName(primaryRouting("reloc").currentNodeId()), equalTo(toNode));

        refresh("reloc");
        assertRecoveredSliceState("reloc", primaryRouting("reloc").currentNodeId());
    }

    /**
     * Establishing a new replica while writes are outstanding: the fresh copy is built by a full peer recovery plus a
     * catch-up of live slice ops. The new replica must be slice-correct, including an op indexed during recovery.
     */
    public void testSliceNewReplicaWithOutstandingIndexing() throws Exception {
        createSliceIndex("newrep", 0);
        indexSliceScenario("newrep");

        setReplicaCount(1, "newrep");
        // An op indexed while the replica is being established must be caught up by the new copy.
        indexDoc("newrep", "sb", "2", "vb2");
        ensureGreen("newrep");

        String replicaNodeId = replicaRouting("newrep").currentNodeId();
        refresh("newrep");
        assertRecoveredSliceState("newrep", replicaNodeId);
        GetResponse late = getDocFromNode("newrep", "sb", "2", replicaNodeId);
        assertThat(late.isExists(), equalTo(true));
        assertThat(late.getSource().get("field"), equalTo("vb2"));
    }

    /**
     * {@code _id} search is slice-context-free: an ids/term query with {@code _slice=_all} matches the id across every
     * slice (returning the plain id from each), while a query scoped to a concrete slice matches only that slice.
     */
    public void testIdSearchIsSliceContextFree() {
        createSliceIndex("ctx", 1);
        indexDoc("ctx", "sa", "1", "va");
        indexDoc("ctx", "sb", "1", "vb");
        refresh("ctx");

        // ids query with _slice=_all returns id "1" from BOTH slices (no slice context needed).
        assertResponse(searchSlice("ctx", SliceIndexing.SLICE_ALL, QueryBuilders.idsQuery().addIds("1")), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(2L));
            for (var hit : r.getHits().getHits()) {
                assertThat(hit.getId(), equalTo("1"));
            }
        });
        // A term query on _id with _slice=_all likewise spans slices.
        assertResponse(
            searchSlice("ctx", SliceIndexing.SLICE_ALL, QueryBuilders.termQuery("_id", "1")),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(2L))
        );
        // Scoped to a concrete slice, the ids query resolves only that slice's document.
        assertResponse(searchSlice("ctx", "sa", QueryBuilders.idsQuery().addIds("1")), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(r.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("va"));
        });
    }

    /**
     * A realtime GET (before refresh, served from the translog) must resolve the slice-scoped document — exercising the
     * compound-term reconstruction in the translog reader — and return the plain id.
     */
    public void testRealtimeGetBeforeRefresh() {
        // Disable periodic refresh so the refresh-count assertion below is deterministic.
        assertAcked(
            prepareCreate("rt").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.refresh_interval", -1)
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping("field", "type=keyword")
        );
        ensureGreen("rt");

        // The first realtime GET flips on translog-location tracking (and refreshes once); ops indexed afterwards carry a
        // translog location, so a realtime GET reads straight from the translog instead of the get-from-searcher fallback.
        // Warm that up before the docs under test so the assertion below exercises the translog read path.
        indexDoc("rt", "sa", "warmup", "w");
        getDoc("rt", "sa", "warmup");

        indexDoc("rt", "sa", "1", "va");
        indexDoc("rt", "sb", "1", "vb");
        long refreshesBefore = refreshCount("rt");
        // Intentionally no refresh: a realtime GET on a slice index resolves each slice's compound _id from the translog.
        GetResponse ga = getDoc("rt", "sa", "1");
        assertThat(ga.isExists(), equalTo(true));
        assertThat(ga.getId(), equalTo("1"));
        assertThat(ga.getSource().get("field"), equalTo("va"));
        GetResponse gb = getDoc("rt", "sb", "1");
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getSource().get("field"), equalTo("vb"));
        // No refresh means the GETs were served from the translog, not the get-from-searcher fallback.
        assertThat("slice realtime GET should read from the translog, not refresh", refreshCount("rt"), equalTo(refreshesBefore));
    }

    private long refreshCount(String index) {
        return indicesAdmin().prepareStats(index).clear().setRefresh(true).get().getTotal().getRefresh().getTotal();
    }

    /**
     * Scroll slicing on {@code _id} must place each document in exactly one partition, even though each doc indexes two
     * {@code _id} terms — so a partitioned scroll covers every document exactly once with no duplicates.
     */
    public void testScrollSlicePartitionsCoverEachDocExactlyOnce() {
        createSliceIndex("scroll", 1);
        final int ids = 10;
        for (int i = 0; i < ids; i++) {
            indexDoc("scroll", "sa", Integer.toString(i), "sa-" + i);
            indexDoc("scroll", "sb", Integer.toString(i), "sb-" + i);
        }
        refresh("scroll");
        final int totalDocs = ids * 2;

        final int partitions = 3;
        final Set<String> seen = new HashSet<>();
        int totalHits = 0;
        for (int p = 0; p < partitions; p++) {
            totalHits += drainScrollSlice("scroll", p, partitions, seen);
        }
        // Every doc returned exactly once: no duplicates (set size == total hits) and full coverage.
        assertThat("a doc appeared in more than one partition", seen.size(), equalTo(totalHits));
        assertThat(seen.size(), equalTo(totalDocs));
    }

    /**
     * Optimistic concurrency ({@code if_seq_no}/{@code if_primary_term}) is scoped per slice: each {@code (slice, id)}
     * has its own seq_no/version lineage, so a conditional write only matches its own slice's document. A plain-id keyed
     * version lookup would let one slice's seq_no satisfy another slice's conditional write.
     */
    public void testOptimisticConcurrencyIsScopedPerSlice() {
        createSliceIndex("occ", 1);
        DocWriteResponse a = indexDoc("occ", "sa", "1", "va");
        DocWriteResponse b = indexDoc("occ", "sb", "1", "vb");
        // The same user id in two slices yields two independent documents, each created (not an update of the other).
        assertThat(a.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(b.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        // Resolve the seq_no/term conflict below against either the live version map or the Lucene index.
        maybeRefresh("occ");

        // Conditionally updating (sa, 1) with the OTHER slice's seq_no/term must conflict: the check resolves against
        // the compound (sa, 1) term, whose current seq_no is a's, not b's. (With plain-id keying b's seq_no would match.)
        VersionConflictEngineException conflict = expectThrows(
            VersionConflictEngineException.class,
            () -> client().index(
                new IndexRequest("occ").id("1")
                    .source("field", "va2")
                    .routing("sa")
                    .setRoutingFromSlice(true)
                    .setIfSeqNo(b.getSeqNo())
                    .setIfPrimaryTerm(b.getPrimaryTerm())
            ).actionGet()
        );
        assertThat(conflict.getMessage(), containsString("version conflict"));

        // Using (sa, 1)'s own seq_no/term succeeds; (sb, 1) updates independently on its own lineage.
        DocWriteResponse updatedA = client().index(
            new IndexRequest("occ").id("1")
                .source("field", "va2")
                .routing("sa")
                .setRoutingFromSlice(true)
                .setIfSeqNo(a.getSeqNo())
                .setIfPrimaryTerm(a.getPrimaryTerm())
        ).actionGet();
        assertThat(updatedA.getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        DocWriteResponse updatedB = client().index(
            new IndexRequest("occ").id("1")
                .source("field", "vb2")
                .routing("sb")
                .setRoutingFromSlice(true)
                .setIfSeqNo(b.getSeqNo())
                .setIfPrimaryTerm(b.getPrimaryTerm())
        ).actionGet();
        assertThat(updatedB.getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        refresh("occ");
        assertThat(getDoc("occ", "sa", "1").getSource().get("field"), equalTo("va2"));
        assertThat(getDoc("occ", "sb", "1").getSource().get("field"), equalTo("vb2"));
    }

    /**
     * Nested children carry their root's compound {@code _id}, so replacing or deleting the root removes the whole
     * block. Children left behind would stay live and corrupt nested queries.
     */
    public void testNestedChildrenAreReplacedWithTheirRoot() {
        assertAcked(
            prepareCreate("nested_idx").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping("nested", "type=nested")
        );
        ensureGreen("nested_idx");

        indexNested("nested_idx", "sa", "1", "a", "b");
        refresh("nested_idx");
        assertThat("root plus two children", liveDocs("nested_idx"), equalTo(3L));
        assertResponse(searchSlice("nested_idx", "sa", nestedQuery("a")), r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L)));

        indexNested("nested_idx", "sa", "1", "c");
        refresh("nested_idx");
        assertThat("root plus one child, the previous children are gone", liveDocs("nested_idx"), equalTo(2L));
        assertResponse(searchSlice("nested_idx", "sa", nestedQuery("a")), r -> assertThat(r.getHits().getTotalHits().value(), equalTo(0L)));
        assertResponse(searchSlice("nested_idx", "sa", nestedQuery("c")), r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L)));

        deleteDoc("nested_idx", "sa", "1");
        refresh("nested_idx");
        assertThat("the whole block is deleted", liveDocs("nested_idx"), equalTo(0L));
    }

    private void indexNested(String index, String slice, String id, String... children) {
        StringBuilder source = new StringBuilder("{\"nested\":[");
        for (int i = 0; i < children.length; i++) {
            source.append(i == 0 ? "" : ",").append("{\"field\":\"").append(children[i]).append("\"}");
        }
        source.append("]}");
        client().index(new IndexRequest(index).id(id).routing(slice).setRoutingFromSlice(true).source(source.toString(), XContentType.JSON))
            .actionGet();
    }

    private static QueryBuilder nestedQuery(String value) {
        return QueryBuilders.nestedQuery("nested", QueryBuilders.termQuery("nested.field", value), ScoreMode.None);
    }

    /** Live Lucene documents on the primary, which includes nested children. */
    private long liveDocs(String index) {
        return indicesAdmin().prepareStats(index).get().getPrimaries().getDocs().getCount();
    }

    /** Drains one scroll-slice partition, recording each hit's source marker, and returns the number of hits seen. */
    private int drainScrollSlice(String index, int sliceId, int max, Set<String> seen) {
        int hits = 0;
        SearchRequestBuilder builder = prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
            .slice(new SliceBuilder(sliceId, max))
            .setScroll(TimeValue.timeValueMinutes(1))
            .setSize(5);
        builder.request().searchSlice(SliceIndexing.SLICE_ALL);
        SearchResponse response = builder.get();
        try {
            String scrollId = response.getScrollId();
            while (response.getHits().getHits().length > 0) {
                for (var hit : response.getHits().getHits()) {
                    seen.add((String) hit.getSourceAsMap().get("field"));
                    hits++;
                }
                response.decRef();
                response = client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).get();
                scrollId = response.getScrollId();
            }
            clearScroll(scrollId);
        } finally {
            response.decRef();
        }
        return hits;
    }
}
