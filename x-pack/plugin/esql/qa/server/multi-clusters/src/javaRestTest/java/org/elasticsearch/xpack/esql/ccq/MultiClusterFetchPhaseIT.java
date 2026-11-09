/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The fetch phase over a remote cluster, whatever the versions of the two clusters. A coordinator plans the fetch phase only
 * when every cluster the query reads supports it, from the minimum transport version that the field caps of each cluster
 * report. With an older remote cluster, the query loads every column eagerly and returns the same rows.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterFetchPhaseIT extends ESRestTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private static final String LOCAL_INDEX = "fetch-local";
    private static final String REMOTE_INDEX = "fetch-remote";
    private static final int DOCS_PER_CLUSTER = 20;
    private static final int LIMIT = 10;
    private static final String QUERY = "FROM "
        + LOCAL_INDEX
        + ","
        + Clusters.REMOTE_CLUSTER_NAME
        + ":"
        + REMOTE_INDEX
        + " | SORT ts DESC | LIMIT "
        + LIMIT
        + " | KEEP ts, payload";

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void createIndices() throws IOException {
        createIndexWithDocs(client(), LOCAL_INDEX, 0, "local-");
        try (RestClient remoteClient = remoteClusterClient()) {
            createIndexWithDocs(remoteClient, REMOTE_INDEX, 1, "remote-");
        }
    }

    @After
    public void deleteIndices() throws IOException {
        deleteIndex(client(), LOCAL_INDEX);
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, REMOTE_INDEX);
        }
    }

    /**
     * The coordinator fetches the winners of the remote cluster from its data nodes when both clusters have the fetch
     * phase, and loads them eagerly when the remote cluster is older. The rows are the same either way.
     */
    public void testTheFetchPhaseFollowsTheVersionOfEveryCluster() throws IOException {
        assumeTrue("an older querying cluster has no fetch phase to turn on", hasTheFetchPhase(client()));
        boolean remoteHasTheFetchPhase;
        try (RestClient remoteClient = remoteClusterClient()) {
            remoteHasTheFetchPhase = hasTheFetchPhase(remoteClient);
        }

        Map<String, Object> response = runWithTheFetchPhase();

        assertThat(response.get("values"), equalTo(expectedRows()));
        if (remoteHasTheFetchPhase) {
            assertThat("documents the coordinator fetched", fetchedDocuments(response), equalTo((long) LIMIT));
        } else {
            assertThat("an older remote cluster keeps the query eager", fetchOperators(response), equalTo(0));
        }
    }

    /**
     * The local documents have even timestamps and the remote ones odd timestamps, so the winners interleave.
     */
    private static void createIndexWithDocs(RestClient client, String index, int parity, String prefix) throws IOException {
        createIndex(client, index, Settings.builder().put("index.number_of_shards", 2).build(), """
            "properties": {
              "ts": { "type": "long" },
              "payload": { "type": "keyword" }
            }
            """, null);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS_PER_CLUSTER; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(Strings.format("{\"ts\": %d, \"payload\": \"%s%d\"}\n", i * 2 + parity, prefix, i));
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertOK(client.performRequest(request));
    }

    private static List<List<Object>> expectedRows() {
        List<List<Object>> all = new ArrayList<>();
        for (int i = 0; i < DOCS_PER_CLUSTER; i++) {
            all.add(List.of(i * 2, "local-" + i));
            all.add(List.of(i * 2 + 1, "remote-" + i));
        }
        all.sort(Comparator.comparing(row -> -((Integer) row.get(0))));
        return all.subList(0, LIMIT);
    }

    private Map<String, Object> runWithTheFetchPhase() throws IOException {
        RestEsqlTestCase.RequestObjectBuilder request = new RestEsqlTestCase.RequestObjectBuilder().query(QUERY).profile(true);
        request.pragmas(Settings.builder().put("fetch_phase", true).build());
        request.pragmasOk();
        return RestEsqlTestCase.runEsqlSync(request, new AssertWarnings.NoWarnings(), profileLogger);
    }

    private static boolean hasTheFetchPhase(RestClient client) {
        return RestEsqlTestCase.hasCapabilities(client, List.of(EsqlCapabilities.Cap.FETCH_PHASE.capabilityName()));
    }

    /**
     * The documents the fetch operators of the coordinator asked for.
     */
    private static long fetchedDocuments(Map<String, Object> response) {
        long documents = 0;
        for (Map<?, ?> operator : fetchOperatorProfiles(response)) {
            documents += ((Number) ((Map<?, ?>) operator.get("status")).get("documents")).longValue();
        }
        assertThat("a fetch operator ran", fetchOperatorProfiles(response).size(), greaterThan(0));
        return documents;
    }

    private static int fetchOperators(Map<String, Object> response) {
        return fetchOperatorProfiles(response).size();
    }

    private static List<Map<?, ?>> fetchOperatorProfiles(Map<String, Object> response) {
        List<Map<?, ?>> fetches = new ArrayList<>();
        for (Object driver : (List<?>) ((Map<?, ?>) response.get("profile")).get("drivers")) {
            for (Object operator : (List<?>) ((Map<?, ?>) driver).get("operators")) {
                if (String.valueOf(((Map<?, ?>) operator).get("operator")).startsWith("FetchOperator")) {
                    fetches.add((Map<?, ?>) operator);
                }
            }
        }
        return fetches;
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
}
