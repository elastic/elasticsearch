/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The fetch phase in a cluster where some nodes have an older version, which can't read its plans. A current coordinator
 * plans eagerly with the pragma or the cluster setting on, so the rows match a query without the fetch phase and nothing
 * is fetched. An older coordinator doesn't know the fetch phase. Once every node is current, as in {@code bcUpgradeTest},
 * the same queries fetch.
 * <p>
 * The cluster accepts the setting only through a current master, which knows it. The tests of the setting move the master
 * where they need it with voting exclusions.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FetchPhaseMixedClusterIT extends ESRestTestCase {
    private static final String INDEX = "fetch_phase_mixed";
    private static final String SETTING = "esql.query.fetch_phase.enabled";
    private static final String FETCH_PHASE = EsqlCapabilities.Cap.FETCH_PHASE.capabilityName();
    /** {@code ts} is unique, so every run returns the same rows in the same order. */
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP ts, payload";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexOnce() throws IOException {
        if (indexExists(INDEX)) {
            return;
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 4, "number_of_replicas": 0},
              "mappings": {"properties": {"ts": {"type": "long"}, "payload": {"type": "keyword"}}}
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int ts = 0; ts < 100; ts++) {
            bulk.append("{\"index\":{}}\n").append(Strings.format("{\"ts\": %s, \"payload\": \"payload-%s\"}\n", ts, ts));
        }
        Request request = new Request("POST", "/" + INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(ObjectPath.createFromResponse(client().performRequest(request)).evaluate("errors"), equalTo(false));
    }

    public void testACurrentCoordinatorWithThePragmaFetchesOnlyWhenEveryNodeCan() throws IOException {
        try (RestClient coordinator = clientFor(false)) {
            assumeTrue("the current nodes have the fetch phase", coordinatorHasTheFetchPhase(coordinator));
            Map<String, Object> withoutTheFetch = query(coordinator, false);
            Map<String, Object> withThePragma = query(coordinator, true);
            assertThat(withThePragma.get("values"), equalTo(withoutTheFetch.get("values")));
            assertFetchedOnlyWhenEveryNodeCan(withThePragma);
        }
    }

    /**
     * An older coordinator ignores the pragma it doesn't know, and loads every column before the cut.
     */
    public void testAnOlderCoordinatorWithThePragmaLoadsEagerly() throws IOException {
        assumeTrue("an older node coordinates", hasOlderNodes());
        try (RestClient coordinator = clientFor(true)) {
            Map<String, Object> withoutTheFetch = query(coordinator, false);
            Map<String, Object> withThePragma = query(coordinator, true);
            assertThat(withThePragma.get("values"), equalTo(withoutTheFetch.get("values")));
            assertThat(fetchOperators(withThePragma), equalTo(0));
        }
    }

    /**
     * An older master doesn't know the setting and rejects it, so the fetch phase can only be turned on for the cluster
     * once the master runs a current version.
     */
    public void testAnOlderMasterRejectsTheSetting() throws Exception {
        assumeTrue("an older node can be the master", hasOlderNodes());
        try (Closeable ignored = masterAmong(true)) {
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(setting("true")));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString(SETTING));
        }
    }

    /**
     * A current master accepts the setting, which turns the fetch phase on for the current coordinators. They fetch only
     * once every node can, and the older nodes go on with the setting in the cluster state.
     */
    public void testACurrentMasterAcceptsTheSetting() throws Exception {
        try (Closeable ignored = masterAmong(false)) {
            client().performRequest(setting("true"));
            try (RestClient coordinator = clientFor(false)) {
                assumeTrue("the current nodes have the fetch phase", coordinatorHasTheFetchPhase(coordinator));
                Map<String, Object> withTheSetting = query(coordinator, false);
                assertFetchedOnlyWhenEveryNodeCan(withTheSetting);
                if (hasOlderNodes()) {
                    try (RestClient olderCoordinator = clientFor(true)) {
                        Map<String, Object> fromAnOlderCoordinator = query(olderCoordinator, false);
                        assertThat(fromAnOlderCoordinator.get("values"), equalTo(withTheSetting.get("values")));
                        assertThat(fetchOperators(fromAnOlderCoordinator), equalTo(0));
                    }
                }
            } finally {
                client().performRequest(setting(null));
            }
        }
    }

    private static Request setting(@Nullable String value) {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": {\"" + SETTING + "\": " + value + "}}");
        return request;
    }

    /**
     * Moves the master to one of the older nodes, or of the current ones, by excluding the others from voting until the
     * returned {@link Closeable} closes.
     */
    private Closeable masterAmong(boolean olderNodes) throws Exception {
        List<String> others = new ArrayList<>();
        ObjectPath nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> all = nodes.evaluate("nodes");
        for (String id : all.keySet()) {
            if (isOlder(nodes.evaluate("nodes." + id + ".version")) != olderNodes) {
                others.add(nodes.evaluate("nodes." + id + ".name"));
            }
        }
        if (others.isEmpty() == false) {
            Request exclude = new Request("POST", "/_cluster/voting_config_exclusions");
            exclude.addParameter("node_names", String.join(",", others));
            client().performRequest(exclude);
        }
        assertBusy(() -> assertThat("the master is among the older nodes", masterIsOlder(), equalTo(olderNodes)));
        return () -> {
            Request clear = new Request("DELETE", "/_cluster/voting_config_exclusions");
            clear.addParameter("wait_for_removal", "false");
            client().performRequest(clear);
        };
    }

    /**
     * With an older node in the cluster nothing is fetched. With every node current the query fetches.
     */
    private void assertFetchedOnlyWhenEveryNodeCan(Map<String, Object> response) throws IOException {
        if (hasCapabilities(adminClient(), List.of(FETCH_PHASE))) {
            assertThat("every node is current, so the query fetches", fetchOperators(response), equalTo(1));
        } else {
            assertThat("an older node can't read the plan, so the coordinator plans eagerly", fetchOperators(response), equalTo(0));
            assertThat(driverDescriptions(response), not(hasItem("fetch")));
        }
    }

    private static Map<String, Object> query(RestClient coordinator, boolean fetchPhasePragma) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", QUERY);
        body.field("profile", true);
        if (fetchPhasePragma) {
            body.field("accept_pragma_risks", true);
            body.startObject("pragma").field("fetch_phase", true).endObject();
        }
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(body.endObject()));
        return entityAsMap(coordinator.performRequest(request));
    }

    private static int fetchOperators(Map<String, Object> response) {
        int fetchOperators = 0;
        for (Map<?, ?> driver : drivers(response)) {
            for (Object operator : (List<?>) driver.get("operators")) {
                if (String.valueOf(((Map<?, ?>) operator).get("operator")).startsWith("FetchOperator")) {
                    fetchOperators++;
                }
            }
        }
        return fetchOperators;
    }

    private static List<Object> driverDescriptions(Map<String, Object> response) {
        return drivers(response).stream().<Object>map(driver -> driver.get("description")).toList();
    }

    private static List<Map<?, ?>> drivers(Map<String, Object> response) {
        List<Map<?, ?>> drivers = new ArrayList<>();
        for (Object driver : (List<?>) ((Map<?, ?>) response.get("profile")).get("drivers")) {
            drivers.add((Map<?, ?>) driver);
        }
        return drivers;
    }

    /**
     * Whether {@code coordinator} has the fetch phase itself, whatever the other nodes have.
     */
    private static boolean coordinatorHasTheFetchPhase(RestClient coordinator) throws IOException {
        Request request = new Request("GET", "_capabilities");
        request.addParameter("method", "POST");
        request.addParameter("path", "/_query");
        request.addParameter("capabilities", FETCH_PHASE);
        request.addParameter("local_only", "true");
        return Boolean.TRUE.equals(entityAsMap(coordinator.performRequest(request)).get("supported"));
    }

    /**
     * A client of the older nodes, or of the current ones.
     */
    private RestClient clientFor(boolean olderNodes) throws IOException {
        ObjectPath nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        String addresses = EsqlDataSourceMixedClusterTestSupport.httpAddressesForCoordinator(nodes, olderNodes);
        return buildClient(restClientSettings(), parseClusterHosts(addresses).toArray(HttpHost[]::new));
    }

    private boolean hasOlderNodes() throws IOException {
        ObjectPath nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> all = nodes.evaluate("nodes");
        for (String id : all.keySet()) {
            if (isOlder(nodes.evaluate("nodes." + id + ".version"))) {
                return true;
            }
        }
        return false;
    }

    private boolean masterIsOlder() throws IOException {
        ObjectPath master = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes/_master")));
        Map<String, Object> nodes = master.evaluate("nodes");
        assertThat(nodes.size(), equalTo(1));
        return isOlder(master.evaluate("nodes." + nodes.keySet().iterator().next() + ".version"));
    }

    private static boolean isOlder(String nodeVersion) {
        return Version.fromString(nodeVersion.replace("-SNAPSHOT", "")).before(Version.CURRENT);
    }
}
