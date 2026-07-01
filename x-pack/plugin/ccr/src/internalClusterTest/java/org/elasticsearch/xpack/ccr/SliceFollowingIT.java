/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * CCR over a slice-enabled index. Verifies that following replicates per-slice {@code _id} semantics end to end: the
 * leader's changes snapshot emits a slice index op as plain id + routing(slice) and a slice delete as the compound
 * {@code (slice, id)} identity term, and the follower's {@code applyTranslogOperation} reconstructs them so the same
 * user id in different slices stays distinct and a delete only removes its own slice.
 */
public class SliceFollowingIT extends CcrSingleNodeTestCase {

    @Before
    public void requireSliceFeatureFlag() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    public void testFollowSliceIndexReplicatesPerSliceOps() throws Exception {
        // Leader is a slice-enabled index. CCR requires soft deletes, which are enabled by default.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("leader")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                )
                .setMapping("field", "type=keyword")
        );
        ensureGreen("leader");

        // The same id "1" in two slices are two distinct docs; "2" lives only in slice sa. Then delete (sa, 1) on the
        // leader before following, so CCR must replay the historical delete too. End state: (sb, 1) and (sa, 2).
        indexDoc("leader", "sa", "1", "va");
        indexDoc("leader", "sb", "1", "vb");
        indexDoc("leader", "sa", "2", "v2");
        assertThat(deleteDoc("leader", "sa", "1").getResult(), equalTo(DocWriteResponse.Result.DELETED));

        client().execute(PutFollowAction.INSTANCE, getPutFollowRequest("leader", "follower")).get();

        // The follower replays the leader's history: each index op (plain id + routing=slice) is re-parsed into the two
        // _id terms, and the delete op (compound uid) reconstructs (id=1, slice=sa) so only sa/1 is removed.
        // GET is realtime, so it reflects replicated ops (incl. deletes) once the follower applies them.
        assertBusy(() -> {
            assertThat(getDoc("follower", "sb", "1").isExists(), equalTo(true));
            assertThat(getDoc("follower", "sa", "2").isExists(), equalTo(true));
            assertThat(getDoc("follower", "sa", "1").isExists(), equalTo(false));
        });
        assertThat(getDoc("follower", "sb", "1").getSource().get("field"), equalTo("vb"));

        // Live replication after following: a fresh slice index op and a slice delete must propagate the same way.
        indexDoc("leader", "sb", "2", "vb2");
        assertThat(deleteDoc("leader", "sb", "1").getResult(), equalTo(DocWriteResponse.Result.DELETED));
        assertBusy(() -> {
            assertThat(getDoc("follower", "sb", "2").isExists(), equalTo(true));
            assertThat(getDoc("follower", "sb", "1").isExists(), equalTo(false));
            assertThat(getDoc("follower", "sa", "2").isExists(), equalTo(true));
        });
        assertThat(getDoc("follower", "sb", "2").getSource().get("field"), equalTo("vb2"));

        ensureEmptyWriteBuffers();
    }

    private DocWriteResponse indexDoc(String index, String slice, String id, String value) {
        return client().index(new IndexRequest(index).id(id).source("field", value).routing(slice).setRoutingFromSlice(true)).actionGet();
    }

    private GetResponse getDoc(String index, String slice, String id) {
        return client().get(new GetRequest(index, id).routing(slice).setRoutingFromSlice(true)).actionGet();
    }

    private DocWriteResponse deleteDoc(String index, String slice, String id) {
        return client().delete(new DeleteRequest(index, id).routing(slice).setRoutingFromSlice(true)).actionGet();
    }
}
