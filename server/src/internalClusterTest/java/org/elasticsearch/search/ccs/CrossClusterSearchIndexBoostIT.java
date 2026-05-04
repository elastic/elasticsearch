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

    private static final String LOCAL_ONLY_INDEX = "ccs_ib_local_only";
    private static final String REMOTE_ONLY_INDEX = "ccs_ib_remote_only";
    private static final String BOTH_CLUSTERS_INDEX = "ccs_ib_shared";

    private static final String ORIGIN_LOCAL = "local";
    private static final String ORIGIN_REMOTE = "remote";
    // ~5% relative tolerance to absorb Lucene scoring noise across two clusters.
    private static final double SCORE_TOLERANCE = 0.05;

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    private static void createIndex(Client client, String index, String origin) {
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

        client.prepareIndex(index).setId("1").setSource(Map.of("k", "v", "origin", origin)).get();
        client.admin().indices().prepareRefresh(index).get();
    }

    /**
     * Setup an index on the local cluster and another on the remote cluster.
     */
    private void setupTwoIndices() {
        createIndex(client(LOCAL_CLUSTER), LOCAL_ONLY_INDEX, ORIGIN_LOCAL);

        setupRemoteOnlyIndex();
    }

    private void setupSharedNameIndices() {
        createIndex(client(LOCAL_CLUSTER), BOTH_CLUSTERS_INDEX, ORIGIN_LOCAL);

        InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        createIndex(client(REMOTE_CLUSTER), BOTH_CLUSTERS_INDEX, ORIGIN_REMOTE);
    }

    private void setupRemoteOnlyIndex() {
        InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(1);
        createIndex(client(REMOTE_CLUSTER), REMOTE_ONLY_INDEX, ORIGIN_REMOTE);
    }

    private SearchRequest makeSearchRequest(int size, boolean mrt, Map<String, Float> indexBoosts, String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.allowPartialSearchResults(false);
        request.setCcsMinimizeRoundtrips(mrt);
        var sb = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(size).trackScores(true).sort("_score", SortOrder.DESC);
        indexBoosts.forEach(sb::indexBoost);
        request.source(sb);
        return request;
    }

    /**
     * Remote-only: search executes on the remote cluster; compare baseline score to boosted score.
     */
    public void testRemoteOnly_Qualified() throws Exception {
        setupRemoteOnlyIndex();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        assertBoostedScore(Map.of(REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX, boost), boost, REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX);
    }

    /**
     * Remote-only: search executes on the remote cluster; compare baseline score to boosted score.
     */
    public void testRemoteOnly_Unqualified() throws Exception {
        setupRemoteOnlyIndex();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        assertBoostedScore(Map.of(REMOTE_ONLY_INDEX, boost), boost, REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX);
    }

    /**
     * Remote-only: mismatched qualified boost entries are ignored on the remote leg.
     */
    public void testRemoteOnly_MissingQualified() throws Exception {
        setupRemoteOnlyIndex();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        assertBoostedScore(
            Map.of("cluster_z:" + REMOTE_ONLY_INDEX, 900.0f, REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX, boost),
            boost,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
    }

    /**
     * Remote-only: {@code _origin:index} does not match the remote cluster alias, so it is ignored on the remote leg.
     */
    public void testRemoteOnly_IgnoresOrigin() throws Exception {
        setupRemoteOnlyIndex();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);

        assertBoostedScore(
            Map.of("_origin:" + REMOTE_ONLY_INDEX, 900.0f, REMOTE_ONLY_INDEX, boost),
            boost,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
    }

    /**
     * Local + remote: {@code cluster:remote_index} boost applies on the remote leg; the local leg keeps its baseline score.
     */
    public void testLocalAndRemote_qualifiedRemoteBoostRaisesRemoteScores() throws Exception {
        setupTwoIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of(REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX, boost),
            LOCAL_ONLY_INDEX,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
        assertLocalRemoteScores(request, true, boost);
    }

    /**
     * Local + remote: {@code _origin:local_index} is resolved on the local CCS leg the same way as an unqualified local index.
     */
    public void testLocalAndRemote_originSyntaxBoostsLocalIndex() throws Exception {
        setupTwoIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of("_origin:" + LOCAL_ONLY_INDEX, boost),
            LOCAL_ONLY_INDEX,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
        assertLocalRemoteScores(request, false, boost);
    }

    public void testLocalAndRemote_unqualifiedLocalBoostRaisesLocalScores() throws Exception {
        setupTwoIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of(LOCAL_ONLY_INDEX, boost),
            LOCAL_ONLY_INDEX,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
        assertLocalRemoteScores(request, false, boost);
    }

    /**
     * Local + remote: only the boost qualified for this cluster's alias should affect the remote index score; bogus
     * qualified entries (wrong cluster alias, missing index) are ignored.
     */
    public void testLocalAndRemote_MissingQualifiedBoostOnRemoteLeg() throws Exception {
        setupTwoIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of(
                "cluster_z:" + REMOTE_ONLY_INDEX,
                800.0f,
                REMOTE_CLUSTER + ":missing",
                800.0f,
                REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX,
                boost
            ),
            LOCAL_ONLY_INDEX,
            REMOTE_CLUSTER + ":" + REMOTE_ONLY_INDEX
        );
        assertLocalRemoteScores(request, true, boost);
    }

    /**
     * Same index name on both clusters: unqualified boost applies on each leg, so scores match after merging.
     */
    public void testSharedIndexName_UnqualifiedBoostEqualScores() throws Exception {
        setupSharedNameIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of(BOTH_CLUSTERS_INDEX, boost),
            BOTH_CLUSTERS_INDEX,
            REMOTE_CLUSTER + ":" + BOTH_CLUSTERS_INDEX
        );
        assertSameBoosts(request);
    }

    /**
     * Same index name on both clusters: {@code _origin:} on the local leg and {@code cluster:} on the remote leg yield the same
     * effective boost on each side after merge.
     */
    public void testSharedIndexName_OriginAndQualifiedRemoteBoostEqualScores() throws Exception {
        setupSharedNameIndices();
        final float boost = randomFloatBetween(50.0f, 100.0f, true);
        SearchRequest request = makeSearchRequest(
            10,
            randomBoolean(),
            Map.of("_origin:" + BOTH_CLUSTERS_INDEX, boost, REMOTE_CLUSTER + ":" + BOTH_CLUSTERS_INDEX, boost),
            BOTH_CLUSTERS_INDEX,
            REMOTE_CLUSTER + ":" + BOTH_CLUSTERS_INDEX
        );
        assertSameBoosts(request);
    }

    private record LocalAndRemoteHits(SearchHit local, SearchHit remote) {}

    private static LocalAndRemoteHits localAndRemoteHits(SearchResponse response) {
        assertHitCount(response, 2);
        SearchHit local = null, remote = null;
        for (SearchHit hit : response.getHits()) {
            String origin = (String) hit.getSourceAsMap().get("origin");
            if (ORIGIN_LOCAL.equals(origin)) {
                assertNull(local);
                local = hit;
            } else if (ORIGIN_REMOTE.equals(origin)) {
                assertNull(remote);
                remote = hit;
            } else {
                fail("unexpected hit origin: " + origin);
            }
        }
        assertNotNull(local);
        assertNotNull(remote);
        return new LocalAndRemoteHits(local, remote);
    }

    /**
     * Assert that local and remote scores are close and get same boosts.
     */
    private void assertSameBoosts(SearchRequest request) throws Exception {
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            LocalAndRemoteHits hits = localAndRemoteHits(response);
            float delta = Math.abs(hits.local.getScore() - hits.remote.getScore());
            assertThat((double) delta, closeTo(0.0, SCORE_TOLERANCE));
        });
    }

    /**
     * Assert index boost for a single index by running non-boosted and boosted searches and comparing the scores.
     */
    private void assertBoostedScore(Map<String, Float> boosts, float expectedBoost, String index) throws Exception {
        boolean mrt = randomBoolean();
        SearchRequest baseline = makeSearchRequest(1, mrt, Map.of(), index);
        final float[] baseScoreHolder = new float[1];
        assertResponse(client(LOCAL_CLUSTER).search(baseline), r -> {
            assertHitCount(r, 1);
            baseScoreHolder[0] = r.getHits().getAt(0).getScore();
        });

        SearchRequest boosted = makeSearchRequest(1, mrt, boosts, index);
        assertResponse(client(LOCAL_CLUSTER).search(boosted), response -> {
            assertHitCount(response, 1);
            assertThat(
                (double) (response.getHits().getAt(0).getScore() / baseScoreHolder[0]),
                closeTo(expectedBoost, expectedBoost * SCORE_TOLERANCE)
            );
        });
    }

    /**
     * Runs {@code request} against the local cluster and asserts that exactly one local and one remote hit came back,
     * and that the boosted side (remote when {@code boostedRemote} is true, otherwise local) scores {@code boost}
     * times higher than the unboosted (baseline) side, modulo a small tolerance for scoring noise.
     */
    private void assertLocalRemoteScores(SearchRequest request, boolean boostedRemote, float boost) throws Exception {
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            LocalAndRemoteHits hits = localAndRemoteHits(response);
            SearchHit boostedHit = boostedRemote ? hits.remote : hits.local;
            SearchHit baselineHit = boostedRemote ? hits.local : hits.remote;
            double ratio = (double) boostedHit.getScore() / baselineHit.getScore();
            assertThat(ratio, closeTo(boost, boost * SCORE_TOLERANCE));
            assertEquals(boostedHit.getId(), response.getHits().getAt(0).getId());
        });
    }
}
