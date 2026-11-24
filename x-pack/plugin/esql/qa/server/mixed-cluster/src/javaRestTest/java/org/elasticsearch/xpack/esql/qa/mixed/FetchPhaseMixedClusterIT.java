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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The fetch phase in a cluster where some nodes lack it. A coordinator with the fetch phase plans eagerly, with the pragma
 * and with the cluster setting, which is on by default in a build with the fetch phase, while a node that can't read fetch
 * requests is in the cluster. So the rows match a query without the fetch phase and nothing is fetched. A coordinator of
 * an older version doesn't know the fetch phase. Once every node has the fetch phase, the same queries fetch.
 * <p>
 * Each node is asked whether it has the fetch phase itself, so the test also runs in a cluster whose old nodes report the
 * current version, like a cluster upgraded from a detached build of main. Whether such an old node reads fetch requests
 * depends on its commit, so the test checks only the rows there until every node has the fetch phase.
 * <p>
 * The cluster accepts the setting only through a master that knows it, one with the fetch phase. The tests of the setting
 * move the master where they need it with voting exclusions.
 * <p>
 * The serverless upgrade tests run this class against a stateless cluster, with their own task that a Gradle filter here
 * doesn't reach, so it skips itself there. In a stateless cluster an index without replicas has no search shard, and
 * voting exclusions don't exist.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FetchPhaseMixedClusterIT extends ESRestTestCase {
    private static final String INDEX = "fetch_phase_mixed";
    private static final String SETTING = "esql.query.fetch_phase.enabled";
    private static final String FETCH_PHASE = EsqlCapabilities.Cap.FETCH_PHASE.capabilityName();
    /** {@code ts} is unique, so every run returns the same rows in the same order. */
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP ts, payload";
    /**
     * A query the setting fetches: a {@code TopN} over more than one shard that defers {@code _source}. The setting keeps
     * {@link #QUERY} eager, since it defers no {@code _source}.
     */
    private static final String SOURCE_QUERY = "FROM " + INDEX + " METADATA _source | SORT ts DESC | LIMIT 10 | KEEP ts, _source";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    /**
     * One node of the cluster.
     *
     * @param olderVersion   whether it reports a version before the current one
     * @param masterEligible whether it can be the master
     * @param hasFetchPhase  whether it has the fetch phase itself
     */
    private record ClusterNode(String name, String httpAddress, boolean olderVersion, boolean masterEligible, boolean hasFetchPhase) {}

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
        // first, so a stateless cluster doesn't get an index it can't search
        assumeFalse("the cluster is stateless", isStateless());
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

    public void testACoordinatorWithTheFetchPhaseAndThePragmaFetchesOnlyWhenEveryNodeCan() throws IOException {
        List<ClusterNode> nodes = nodes();
        try (RestClient coordinator = clientOf(randomFrom(withTheFetchPhase(nodes)))) {
            Map<String, Object> withoutTheFetch = query(coordinator, QUERY, false);
            Map<String, Object> withThePragma = query(coordinator, QUERY, true);
            assertThat(withThePragma.get("values"), equalTo(withoutTheFetch.get("values")));
            assertFetchedOnlyWhenEveryNodeCan(nodes, withThePragma);
        }
    }

    /**
     * The setting is on by default in a build with the fetch phase, so a coordinator with the fetch phase plans the fetch
     * for a query that sets neither the pragma nor the setting. It fetches only once every node can.
     */
    public void testACoordinatorWithTheFetchPhaseFetchesByDefaultOnlyWhenEveryNodeCan() throws IOException {
        List<ClusterNode> nodes = nodes();
        try (RestClient coordinator = clientOf(randomFrom(withTheFetchPhase(nodes)))) {
            Map<String, Object> withoutTheFetch = query(coordinator, SOURCE_QUERY, false);
            Map<String, Object> byDefault = query(coordinator, SOURCE_QUERY, null);
            assertThat(byDefault.get("values"), equalTo(withoutTheFetch.get("values")));
            assertFetchedOnlyWhenEveryNodeCan(nodes, byDefault);
        }
    }

    /**
     * A coordinator of an older version ignores the pragma it doesn't know, and loads every column before the cut. An old
     * node of a detached build may know the pragma, so it doesn't count here.
     */
    public void testAnOlderCoordinatorWithThePragmaLoadsEagerly() throws IOException {
        List<ClusterNode> olderVersion = nodes().stream().filter(ClusterNode::olderVersion).toList();
        assumeFalse("a node of an older version coordinates", olderVersion.isEmpty());
        try (RestClient coordinator = clientOf(randomFrom(olderVersion))) {
            Map<String, Object> withoutTheFetch = query(coordinator, QUERY, null);
            Map<String, Object> withThePragma = query(coordinator, QUERY, true);
            assertThat(withThePragma.get("values"), equalTo(withoutTheFetch.get("values")));
            assertThat(fetchOperators(withThePragma), equalTo(0));
        }
    }

    /**
     * A master without the fetch phase doesn't know the setting and rejects any value, so a cluster can change the setting
     * only once the master has the fetch phase.
     */
    public void testAMasterWithoutTheFetchPhaseRejectsTheSetting() throws Exception {
        List<ClusterNode> nodes = nodes();
        // without the feature flag no node reports the fetch phase, although the current ones know the setting
        assumeTrue("a node has the fetch phase", nodes.stream().anyMatch(ClusterNode::hasFetchPhase));
        List<ClusterNode> without = nodes.stream().filter(node -> node.hasFetchPhase() == false && node.masterEligible()).toList();
        assumeFalse("a node without the fetch phase can be the master", without.isEmpty());
        try (Closeable ignored = masterAmong(nodes, without)) {
            String value = randomFrom("true", "false");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(setting(value)));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString(SETTING));
        }
    }

    /**
     * A master with the fetch phase accepts the setting. Turning it off keeps the coordinators with the fetch phase eager,
     * also once every node can fetch, and the nodes without it go on with the setting in the cluster state.
     */
    public void testAMasterWithTheFetchPhaseAcceptsTheSetting() throws Exception {
        List<ClusterNode> nodes = nodes();
        List<ClusterNode> with = withTheFetchPhase(nodes);
        List<ClusterNode> masters = with.stream().filter(ClusterNode::masterEligible).toList();
        assumeFalse("a node with the fetch phase can be the master", masters.isEmpty());
        try (Closeable ignored = masterAmong(nodes, masters)) {
            client().performRequest(setting("false"));
            try (RestClient coordinator = clientOf(randomFrom(with))) {
                Map<String, Object> withTheSettingOff = query(coordinator, SOURCE_QUERY, null);
                assertThat(fetchOperators(withTheSettingOff), equalTo(0));
                assertThat(driverDescriptions(withTheSettingOff), not(hasItem("fetch")));
                for (ClusterNode without : nodes) {
                    if (without.hasFetchPhase()) {
                        continue;
                    }
                    try (RestClient otherCoordinator = clientOf(without)) {
                        Map<String, Object> fromANodeWithout = query(otherCoordinator, SOURCE_QUERY, null);
                        assertThat(fromANodeWithout.get("values"), equalTo(withTheSettingOff.get("values")));
                        assertThat(fetchOperators(fromANodeWithout), equalTo(0));
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
     * Moves the master to one of {@code candidates}, by excluding the other master-eligible nodes from voting until the
     * returned {@link Closeable} closes.
     */
    private Closeable masterAmong(List<ClusterNode> nodes, List<ClusterNode> candidates) throws Exception {
        List<String> names = candidates.stream().map(ClusterNode::name).toList();
        List<String> others = nodes.stream()
            .filter(ClusterNode::masterEligible)
            .map(ClusterNode::name)
            .filter(name -> names.contains(name) == false)
            .toList();
        if (others.isEmpty() == false) {
            Request exclude = new Request("POST", "/_cluster/voting_config_exclusions");
            exclude.addParameter("node_names", String.join(",", others));
            client().performRequest(exclude);
        }
        assertBusy(() -> assertThat("the master is among " + names, names, hasItem(masterName())));
        return () -> {
            Request clear = new Request("DELETE", "/_cluster/voting_config_exclusions");
            clear.addParameter("wait_for_removal", "false");
            client().performRequest(clear);
        };
    }

    /**
     * With a node that can't read fetch requests in the cluster nothing is fetched. With every node having the fetch phase
     * the query fetches.
     */
    private void assertFetchedOnlyWhenEveryNodeCan(List<ClusterNode> nodes, Map<String, Object> response) {
        Integer expected = expectedFetchOperators(nodes);
        if (expected == null) {
            return;
        }
        assertThat(fetchOperators(response), equalTo(expected));
        if (expected == 0) {
            assertThat(driverDescriptions(response), not(hasItem("fetch")));
        }
    }

    /**
     * The fetch operators of a query that a coordinator with the fetch phase plans with it, or {@code null} when the test
     * can't tell. One once every node has the fetch phase. None while a node of an older version is in the cluster, since
     * it can't read fetch requests. The test can't tell when every node reports the current version but some lack the
     * fetch phase: those run a detached build of main, which may read fetch requests before it reports the fetch phase.
     */
    @Nullable
    private static Integer expectedFetchOperators(List<ClusterNode> nodes) {
        if (nodes.stream().allMatch(ClusterNode::hasFetchPhase)) {
            return 1;
        }
        return nodes.stream().anyMatch(ClusterNode::olderVersion) ? 0 : null;
    }

    /**
     * @param fetchPhasePragma the {@code fetch_phase} pragma, {@code null} to follow the cluster setting
     */
    private static Map<String, Object> query(RestClient coordinator, String query, @Nullable Boolean fetchPhasePragma) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", query);
        body.field("profile", true);
        if (fetchPhasePragma != null) {
            body.field("accept_pragma_risks", true);
            body.startObject("pragma").field("fetch_phase", fetchPhasePragma).endObject();
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
     * The nodes of the cluster, each asked whether it has the fetch phase itself.
     */
    private List<ClusterNode> nodes() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> all = info.evaluate("nodes");
        List<ClusterNode> nodes = new ArrayList<>();
        for (String id : all.keySet()) {
            String name = info.evaluate("nodes." + id + ".name");
            String httpAddress = info.evaluate("nodes." + id + ".http.publish_address");
            boolean olderVersion = isOlder(info.evaluate("nodes." + id + ".version"));
            List<String> roles = info.evaluate("nodes." + id + ".roles");
            nodes.add(new ClusterNode(name, httpAddress, olderVersion, roles.contains("master"), hasTheFetchPhase(httpAddress)));
        }
        return nodes;
    }

    /**
     * The nodes with the fetch phase. Skips the test without one, as in a build without the feature flag.
     */
    private static List<ClusterNode> withTheFetchPhase(List<ClusterNode> nodes) {
        List<ClusterNode> with = nodes.stream().filter(ClusterNode::hasFetchPhase).toList();
        assumeFalse("a node has the fetch phase", with.isEmpty());
        return with;
    }

    /**
     * Whether the node at {@code httpAddress} has the fetch phase itself, whatever the other nodes have. A node too old to
     * know {@code local_only} answers 4xx, and has no fetch phase either.
     */
    private boolean hasTheFetchPhase(String httpAddress) throws IOException {
        Request request = new Request("GET", "_capabilities");
        request.addParameter("method", "POST");
        request.addParameter("path", "/_query");
        request.addParameter("capabilities", FETCH_PHASE);
        request.addParameter("local_only", "true");
        try (RestClient node = clientOf(httpAddress)) {
            return Boolean.TRUE.equals(entityAsMap(node.performRequest(request)).get("supported"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() / 100 == 4) {
                return false;
            }
            throw e;
        }
    }

    private RestClient clientOf(ClusterNode node) throws IOException {
        return clientOf(node.httpAddress());
    }

    private RestClient clientOf(String httpAddress) throws IOException {
        return buildClient(restClientSettings(), parseClusterHosts(httpAddress).toArray(HttpHost[]::new));
    }

    private String masterName() throws IOException {
        ObjectPath master = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes/_master")));
        Map<String, Object> nodes = master.evaluate("nodes");
        assertThat(nodes.size(), equalTo(1));
        return master.evaluate("nodes." + nodes.keySet().iterator().next() + ".name");
    }

    private static boolean isOlder(String nodeVersion) {
        return Version.fromString(nodeVersion.replace("-SNAPSHOT", "")).before(Version.CURRENT);
    }

    /**
     * Whether the cluster is stateless: some node has the role of indexing or searching shards.
     */
    private static boolean isStateless() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> nodes = info.evaluate("nodes");
        for (String id : nodes.keySet()) {
            List<String> roles = info.evaluate("nodes." + id + ".roles");
            if (roles.contains(DiscoveryNodeRole.INDEX_ROLE.roleName()) || roles.contains(DiscoveryNodeRole.SEARCH_ROLE.roleName())) {
                return true;
            }
        }
        return false;
    }
}
