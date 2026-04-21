/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * End-to-end {@code indices_boost} checks for cross-cluster search.
 */
public class CrossClusterSearchIndexBoostIT extends AbstractCrossClusterSearchTestCase {

    private static final String LOCAL_ONLY = "ccs_ib_local_only";
    private static final String REMOTE_ONLY = "ccs_ib_remote_only";
    private static final String SHARED_NAME = "ccs_ib_shared";

    private static final String ORIGIN_LOCAL = "local";
    private static final String ORIGIN_REMOTE = "remote";

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    private static void indexOneDocument(Client client, String index, String origin) {
        client.prepareIndex(index).setId("1").setSource(Map.of("k", "v", "origin", origin)).get();
        client.admin().indices().prepareRefresh(index).get();
    }

    private static void createEmptyIndex(Client client, String index) {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword", "origin", "type=keyword")
        );
        assertFalse(
            client.admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, index)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
    }

    /**
     * Mirror index names on both clusters so every unqualified {@code indices_boost} entry resolves on each leg.
     */
    private void setupDistinctIndices() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(LOCAL_ONLY)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword", "origin", "type=keyword")
        );
        indexOneDocument(client(LOCAL_CLUSTER), LOCAL_ONLY, ORIGIN_LOCAL);
        createEmptyIndex(client(LOCAL_CLUSTER), REMOTE_ONLY);

        InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        createEmptyIndex(client(REMOTE_CLUSTER), LOCAL_ONLY);
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(REMOTE_ONLY)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword", "origin", "type=keyword")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, REMOTE_ONLY)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexOneDocument(client(REMOTE_CLUSTER), REMOTE_ONLY, ORIGIN_REMOTE);
    }

    private void setupSharedNameIndices() {
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(SHARED_NAME)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword", "origin", "type=keyword")
        );
        indexOneDocument(client(LOCAL_CLUSTER), SHARED_NAME, ORIGIN_LOCAL);

        InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(SHARED_NAME)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword", "origin", "type=keyword")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, SHARED_NAME)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexOneDocument(client(REMOTE_CLUSTER), SHARED_NAME, ORIGIN_REMOTE);
    }

    private void setupRemoteOnlyIndex() {
        InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(REMOTE_ONLY)
                .setSettings(indexSettings(1, 0))
                .setMapping("k", "type=keyword")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, REMOTE_ONLY)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        client(REMOTE_CLUSTER).prepareIndex(REMOTE_ONLY).setId("1").setSource("k", "v").get();
        client(REMOTE_CLUSTER).admin().indices().prepareRefresh(REMOTE_ONLY).get();
    }

    private SearchRequest makeSearchRequest(int size, Map<String, Float> indexBoosts, String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.allowPartialSearchResults(false);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        var sb = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(size).trackScores(true).sort("_score", SortOrder.DESC);
        indexBoosts.forEach(sb::indexBoost);
        request.source(sb);
        return request;
    }

    /**
     * Remote-only: search executes on the remote cluster; compare baseline score to boosted score.
     */
    public void testRemoteOnly_Unqualified() throws Exception {
        setupRemoteOnlyIndex();
        SearchRequest baseline = makeSearchRequest(1, Map.of(), REMOTE_CLUSTER + ":" + REMOTE_ONLY);
        final float[] baseScoreHolder = new float[1];
        assertResponse(client(LOCAL_CLUSTER).search(baseline), r -> {
            assertHitCount(r, 1);
            baseScoreHolder[0] = r.getHits().getAt(0).getScore();
        });

        SearchRequest boosted = makeSearchRequest(1, Map.of(REMOTE_ONLY, 22.0f), REMOTE_CLUSTER + ":" + REMOTE_ONLY);
        assertResponse(client(LOCAL_CLUSTER).search(boosted), response -> {
            assertHitCount(response, 1);
            assertThat((double) (response.getHits().getAt(0).getScore() / baseScoreHolder[0]), closeTo(22.0, 0.3));
        });
    }

    /**
     * Remote-only: mismatched qualified boost entries are ignored on the remote leg.
     */
    public void testRemoteOnly_MissingQualified() throws Exception {
        setupRemoteOnlyIndex();
        SearchRequest baseline = makeSearchRequest(1, Map.of(), REMOTE_CLUSTER + ":" + REMOTE_ONLY);
        final float[] baseScoreHolder = new float[1];
        assertResponse(client(LOCAL_CLUSTER).search(baseline), r -> {
            assertHitCount(r, 1);
            baseScoreHolder[0] = r.getHits().getAt(0).getScore();
        });

        SearchRequest boosted = makeSearchRequest(
            1,
            Map.of("cluster_z:" + REMOTE_ONLY, 900.0f, REMOTE_CLUSTER + ":" + REMOTE_ONLY, 6.0f),
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(boosted), response -> {
            assertHitCount(response, 1);
            assertThat((double) (response.getHits().getAt(0).getScore() / baseScoreHolder[0]), closeTo(6.0, 0.3));
        });
    }

    /**
     * Remote-only: {@code _origin:index} does not match the remote cluster alias, so it is ignored on the remote leg.
     */
    public void testRemoteOnly_IgnoresOrigin() throws Exception {
        setupRemoteOnlyIndex();
        SearchRequest baseline = makeSearchRequest(1, Map.of(), REMOTE_CLUSTER + ":" + REMOTE_ONLY);
        final float[] baseScoreHolder = new float[1];
        assertResponse(client(LOCAL_CLUSTER).search(baseline), r -> {
            assertHitCount(r, 1);
            baseScoreHolder[0] = r.getHits().getAt(0).getScore();
        });

        SearchRequest boosted = makeSearchRequest(
            1,
            Map.of("_origin:" + REMOTE_ONLY, 900.0f, REMOTE_ONLY, 8.0f),
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(boosted), response -> {
            assertHitCount(response, 1);
            assertThat((double) (response.getHits().getAt(0).getScore() / baseScoreHolder[0]), closeTo(8.0, 0.3));
        });
    }

    /**
     * Local + remote: {@code cluster:remote_index} boost applies on the remote leg; unqualified local boost on the local leg.
     */
    public void testLocalAndRemote_qualifiedRemoteBoostRaisesRemoteScores() throws Exception {
        setupDistinctIndices();
        final float localBoost = 1.0f;
        final float remoteBoost = 34.0f;
        SearchRequest request = makeSearchRequest(
            10,
            Map.of(LOCAL_ONLY, localBoost, REMOTE_CLUSTER + ":" + REMOTE_ONLY, remoteBoost),
            LOCAL_ONLY,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> assertLocalRemoteScores(response, localBoost, remoteBoost));
    }

    /**
     * Local + remote: {@code _origin:local_index} is resolved on the local CCS leg the same way as an unqualified local index.
     */
    public void testLocalAndRemote_originSyntaxBoostsLocalIndex() throws Exception {
        setupDistinctIndices();
        final float localBoost = 30.0f;
        final float remoteBoost = 1.0f;
        SearchRequest request = makeSearchRequest(
            10,
            Map.of("_origin:" + LOCAL_ONLY, localBoost, REMOTE_CLUSTER + ":" + REMOTE_ONLY, remoteBoost),
            LOCAL_ONLY,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> assertLocalRemoteScores(response, localBoost, remoteBoost));
    }

    public void testLocalAndRemote_unqualifiedLocalBoostRaisesLocalScores() throws Exception {
        setupDistinctIndices();
        final float localBoost = 30.0f;
        final float remoteBoost = 1.0f;
        SearchRequest request = makeSearchRequest(
            10,
            Map.of(LOCAL_ONLY, localBoost, REMOTE_CLUSTER + ":" + REMOTE_ONLY, remoteBoost),
            LOCAL_ONLY,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> assertLocalRemoteScores(response, localBoost, remoteBoost));
    }

    /**
     * Local + remote: only the boost qualified for this cluster's alias should affect the remote index score.
     */
    public void testLocalAndRemote_MissingQualifiedBoostOnRemoteLeg() throws Exception {
        setupDistinctIndices();
        final float localBoost = 1.0f;
        final float remoteBoost = 7.0f;
        SearchRequest request = makeSearchRequest(
            10,
            Map.of(
                LOCAL_ONLY,
                localBoost,
                "cluster_z:" + REMOTE_ONLY,
                800.0f,
                REMOTE_CLUSTER + ":missing",
                800.0f,
                REMOTE_CLUSTER + ":" + REMOTE_ONLY,
                remoteBoost
            ),
            LOCAL_ONLY,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY
        );
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> assertLocalRemoteScores(response, localBoost, remoteBoost));
    }

    /**
     * Same index name on both clusters: unqualified boost applies on each leg, so scores match after merging.
     */
    public void testSharedIndexName_UnqualifiedBoostEqualScores() throws Exception {
        setupSharedNameIndices();
        final float boost = 13.0f;
        SearchRequest request = makeSearchRequest(10, Map.of(SHARED_NAME, boost), SHARED_NAME, REMOTE_CLUSTER + ":" + SHARED_NAME);
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            assertHitCount(response, 2);
            SearchHit localHit = null;
            SearchHit remoteHit = null;
            for (SearchHit hit : response.getHits()) {
                String origin = (String) hit.getSourceAsMap().get("origin");
                if (ORIGIN_LOCAL.equals(origin)) {
                    assertNull(localHit);
                    localHit = hit;
                } else if (ORIGIN_REMOTE.equals(origin)) {
                    assertNull(remoteHit);
                    remoteHit = hit;
                } else {
                    fail("unexpected hit origin: " + origin);
                }
            }
            assertNotNull(localHit);
            assertNotNull(remoteHit);
            float delta = Math.abs(localHit.getScore() - remoteHit.getScore());
            assertThat((double) delta, closeTo(0.0, 0.08));
        });
    }

    /**
     * Same index name on both clusters: {@code _origin:} on the local leg and {@code cluster:} on the remote leg yield the same
     * effective boost on each side after merge.
     */
    public void testSharedIndexName_OriginAndQualifiedRemoteBoostEqualScores() throws Exception {
        setupSharedNameIndices();
        final float boost = 11.0f;
        SearchRequest request = makeSearchRequest(
            10,
            Map.of("_origin:" + SHARED_NAME, boost, REMOTE_CLUSTER + ":" + SHARED_NAME, boost),
            SHARED_NAME,
            REMOTE_CLUSTER + ":" + SHARED_NAME
        );
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            assertHitCount(response, 2);
            SearchHit localHit = null;
            SearchHit remoteHit = null;
            for (SearchHit hit : response.getHits()) {
                String origin = (String) hit.getSourceAsMap().get("origin");
                if (ORIGIN_LOCAL.equals(origin)) {
                    assertNull(localHit);
                    localHit = hit;
                } else if (ORIGIN_REMOTE.equals(origin)) {
                    assertNull(remoteHit);
                    remoteHit = hit;
                } else {
                    fail("unexpected hit origin: " + origin);
                }
            }
            assertNotNull(localHit);
            assertNotNull(remoteHit);
            float delta = Math.abs(localHit.getScore() - remoteHit.getScore());
            assertThat((double) delta, closeTo(0.0, 0.08));
        });
    }

    private static void assertLocalRemoteScores(SearchResponse response, float expectedLocalBoost, float expectedRemoteBoost) {
        assertHitCount(response, 2);
        SearchHit localHit = null;
        SearchHit remoteHit = null;
        for (SearchHit hit : response.getHits()) {
            String origin = (String) hit.getSourceAsMap().get("origin");
            if (ORIGIN_LOCAL.equals(origin)) {
                assertNull(localHit);
                localHit = hit;
            } else if (ORIGIN_REMOTE.equals(origin)) {
                assertNull(remoteHit);
                remoteHit = hit;
            } else {
                fail("unexpected hit origin: " + origin);
            }
        }
        assertNotNull(localHit);
        assertNotNull(remoteHit);
        double ratio = remoteHit.getScore() / localHit.getScore();
        assertThat(ratio, closeTo(expectedRemoteBoost / expectedLocalBoost, 0.35));
        if (expectedRemoteBoost > expectedLocalBoost) {
            assertEquals(remoteHit.getId(), response.getHits().getAt(0).getId());
        } else if (expectedLocalBoost > expectedRemoteBoost) {
            assertEquals(localHit.getId(), response.getHits().getAt(0).getId());
        }
    }
}
