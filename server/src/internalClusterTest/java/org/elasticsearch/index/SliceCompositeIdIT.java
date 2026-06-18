/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.DocWriteResponse;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
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

    private void createSliceIndex(String index, int replicas) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", replicas)
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
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

    private DocWriteResponse deleteDoc(String index, String slice, String id) {
        return client().delete(new DeleteRequest(index, id).routing(slice).setRoutingFromSlice(true)).actionGet();
    }

    private SearchRequestBuilder searchSlice(String index, String slice, QueryBuilder query) {
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

        // GET within each slice returns that slice's document, with the plain id.
        GetResponse ga = getDoc("idx", "sa", "1");
        assertThat(ga.isExists(), equalTo(true));
        assertThat(ga.getId(), equalTo("1"));
        assertThat(ga.getSource().get("field"), equalTo("va"));
        GetResponse gb = getDoc("idx", "sb", "1");
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));

        // A match-all search scoped to a slice sees only that slice's document; _all sees both.
        assertResponse(searchSlice("idx", "sa", QueryBuilders.matchAllQuery()), r -> {
            assertThat(r.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(r.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(r.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("va"));
        });
        assertResponse(
            searchSlice("idx", SliceIndexing.SLICE_ALL, QueryBuilders.matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(2L))
        );

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
     * Update-by-id is scoped to the slice: updating id "1" in one slice must not affect id "1" in another slice.
     */
    public void testUpdateByIdIsScopedToSlice() {
        createSliceIndex("upd", 1);
        indexDoc("upd", "sa", "1", "va");
        indexDoc("upd", "sb", "1", "vb");
        refresh("upd");

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
        refresh("bulk");

        assertThat(getDoc("bulk", "sa", "1").getSource().get("field"), equalTo("va"));
        assertThat(getDoc("bulk", "sb", "1").getSource().get("field"), equalTo("vb"));

        // Bulk update of one slice and delete of the other, both targeting id "1".
        BulkRequest mutateBulk = new BulkRequest();
        mutateBulk.add(new UpdateRequest("bulk", "1").doc("field", "va2").routing("sa").setRoutingFromSlice(true));
        mutateBulk.add(new DeleteRequest("bulk", "1").routing("sb").setRoutingFromSlice(true));
        BulkResponse mutateResponse = client().bulk(mutateBulk).actionGet();
        assertThat(mutateResponse.buildFailureMessage(), mutateResponse.hasFailures(), equalTo(false));
        refresh("bulk");

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
            containsString("[_slice] is required when [index.slice.enabled] is true")
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
     * Slice-scoped index and delete operations must survive recovery: a full cluster restart replays the translog (and the
     * replica is rebuilt via peer recovery). The recovered shard must reflect that the delete targeted only its slice's
     * composite term, leaving the same id in another slice intact.
     */
    public void testSliceDocsSurviveFullRestart() throws Exception {
        createSliceIndex("rec", 1);

        indexDoc("rec", "sa", "1", "va");
        indexDoc("rec", "sb", "1", "vb");
        // Delete only slice sa's document. Intentionally do not flush, so the index+delete ops are replayed from the translog
        // on recovery — exercising the delete-translog-replay path against the composite term.
        deleteDoc("rec", "sa", "1");
        refresh("rec");

        internalCluster().fullRestart();
        ensureGreen("rec");

        assertThat(getDoc("rec", "sa", "1").isExists(), equalTo(false));
        GetResponse gb = getDoc("rec", "sb", "1");
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getId(), equalTo("1"));
        assertThat(gb.getSource().get("field"), equalTo("vb"));

        assertResponse(
            searchSlice("rec", SliceIndexing.SLICE_ALL, QueryBuilders.matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L))
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
        createSliceIndex("rt", 1);
        indexDoc("rt", "sa", "1", "va");
        indexDoc("rt", "sb", "1", "vb");
        // Intentionally no refresh: GET is realtime and reads from the translog.

        GetResponse ga = getDoc("rt", "sa", "1");
        assertThat(ga.isExists(), equalTo(true));
        assertThat(ga.getId(), equalTo("1"));
        assertThat(ga.getSource().get("field"), equalTo("va"));
        GetResponse gb = getDoc("rt", "sb", "1");
        assertThat(gb.isExists(), equalTo(true));
        assertThat(gb.getSource().get("field"), equalTo("vb"));
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
