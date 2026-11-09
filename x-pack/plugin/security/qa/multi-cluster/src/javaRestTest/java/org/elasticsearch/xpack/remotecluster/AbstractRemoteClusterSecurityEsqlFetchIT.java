/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The ES|QL fetch phase over a remote cluster with security, under one of the two security models of remote clusters.
 * <p>
 * The fulfilling cluster has three nodes. The coordinator of the querying cluster reaches it through {@link #GATEWAY_NODE},
 * which holds no index, so every fetch, keep-alive and free of the coordinator goes through that gateway node to the node
 * that holds the contexts. The gateway authorizes the forwarded request by its action, and the target node authorizes the
 * request like an index read of the query, with the roles the user has on the fulfilling cluster at that time.
 * <p>
 * The tests that change a role between the phases stall the query phase of {@link #SLOW_INDEX} on the querying cluster with
 * {@code error_query}. The remote data node answered meanwhile and holds the contexts of its winners, and the coordinator
 * fetches from it once the stalled shard answers. Like a local query, FLS follows the role at fetch time, DLS keeps the
 * documents of the query phase, and losing read fails the query.
 */
public abstract class AbstractRemoteClusterSecurityEsqlFetchIT extends AbstractRemoteClusterSecurityTestCase {
    /** The index of each node of the fulfilling cluster, in the order the cluster declares them. */
    protected static final int GATEWAY_NODE_INDEX = 0;
    protected static final int DATA_NODE_INDEX = 1;
    protected static final int SLOW_NODE_INDEX = 2;
    protected static final String FULFILLING_CLUSTER_NAME = "fulfilling-cluster";
    private static final String GATEWAY_NODE = FULFILLING_CLUSTER_NAME + "-" + GATEWAY_NODE_INDEX;
    private static final String DATA_NODE = FULFILLING_CLUSTER_NAME + "-" + DATA_NODE_INDEX;
    private static final String SLOW_NODE = FULFILLING_CLUSTER_NAME + "-" + SLOW_NODE_INDEX;

    /** The user of the queries. Each test gives {@link #ROLE} the privileges it needs. */
    protected static final String FETCH_USER = "fetch_user";
    protected static final String ROLE = "fetch_role";

    /** On {@link #DATA_NODE}. */
    private static final String INDEX_A = "fetch-a";
    private static final List<List<Object>> DOCS_OF_A = List.of(
        List.of(10, "sales"),
        List.of(20, "engineering"),
        List.of(30, "sales"),
        List.of(40, "engineering")
    );
    /** On {@link #DATA_NODE}, with values between those of {@link #INDEX_A}. */
    private static final String INDEX_B = "fetch-b";
    private static final List<List<Object>> DOCS_OF_B = List.of(List.of(15, "sales"), List.of(25, "engineering"), List.of(35, "sales"));
    /** On {@link #SLOW_NODE}, with values below all others, so its rows lose the cut of the remote coordinator. */
    private static final String INDEX_C = "fetch-c";
    private static final List<List<Object>> DOCS_OF_C = List.of(List.of(3, "sales"), List.of(4, "engineering"));
    /** On the querying cluster, with values below all others, so its rows lose the cut. */
    protected static final String SLOW_INDEX = "fetch-slow";
    private static final List<List<Object>> DOCS_OF_SLOW = List.of(List.of(1, "sales"), List.of(2, "engineering"));
    /** Long enough to change a role, short enough for a test. */
    private static final int STALL_SECONDS = 5;

    private static final String FETCH_ACTION = "indices:data/read/esql/fetch";
    private static final String PROXY_ACTION_PREFIX = "internal:transport/proxy/";

    private static final String A_AND_B = "FROM "
        + REMOTE_CLUSTER_ALIAS
        + ":"
        + INDEX_A
        + ","
        + REMOTE_CLUSTER_ALIAS
        + ":"
        + INDEX_B
        + " | SORT value DESC | LIMIT 3 | KEEP value, org";
    private static final String A_AND_C = "FROM "
        + REMOTE_CLUSTER_ALIAS
        + ":"
        + INDEX_A
        + ","
        + REMOTE_CLUSTER_ALIAS
        + ":"
        + INDEX_C
        + " | SORT value DESC | LIMIT 2 | KEEP value, org";
    private static final String A_AND_SLOW = "FROM "
        + REMOTE_CLUSTER_ALIAS
        + ":"
        + INDEX_A
        + ","
        + SLOW_INDEX
        + " | SORT value DESC | LIMIT 2 | KEEP value, org";

    /**
     * Connects the querying cluster to the fulfilling cluster through {@link #GATEWAY_NODE}.
     */
    protected abstract void configureRemoteClusterThroughTheGateway() throws Exception;

    /**
     * Gives {@link #FETCH_USER} the index privileges {@code indexPrivileges} on the indices of the fulfilling cluster, where
     * the security model of the subclass reads them from, and read on {@link #SLOW_INDEX} of the querying cluster.
     */
    protected abstract void putRole(String... indexPrivileges) throws IOException;

    @Before
    public void connectAndCreateTheUser() throws Exception {
        configureRemoteClusterThroughTheGateway();
        assumeTrue(
            "the querying cluster runs the fetch phase",
            clusterHasCapability("POST", "/_query", List.of(), List.of("fetch_phase")).orElse(false)
        );
        Request putUser = new Request("PUT", "/_security/user/" + FETCH_USER);
        putUser.setJsonEntity(Strings.format("""
            {"password": "%s", "roles": ["%s"]}""", PASS, ROLE));
        assertOK(adminClient().performRequest(putUser));
    }

    @After
    public void checkNoContextIsLeftAndDeleteIndices() throws Exception {
        // checks before the delete, which closes the contexts of the deleted indices
        try {
            assertBusy(() -> {
                assertThat("open contexts on " + DATA_NODE, openContexts(DATA_NODE), equalTo(0));
                assertThat("open contexts on " + SLOW_NODE, openContexts(SLOW_NODE), equalTo(0));
                Response local = adminClient().performRequest(new Request("GET", "/_nodes/stats/indices/search"));
                Map<String, Object> nodes = ObjectPath.createFromResponse(local).evaluate("nodes");
                for (Object stats : nodes.values()) {
                    assertThat(
                        "open contexts on the querying cluster",
                        ObjectPath.evaluate(stats, "indices.search.open_contexts"),
                        equalTo(0)
                    );
                }
            });
        } finally {
            Request delete = new Request("DELETE", "/fetch-*");
            delete.addParameter("ignore_unavailable", "true");
            assertOK(performRequestAgainstFulfillingCluster(delete));
            Request deleteSlow = new Request("DELETE", "/" + SLOW_INDEX);
            deleteSlow.addParameter("ignore_unavailable", "true");
            assertOK(adminClient().performRequest(deleteSlow));
        }
    }

    /**
     * The gateway node forwards the fetch of the winners to the data node, which loads them as an index read of the user.
     */
    public void testTheGatewayForwardsTheFetchOfTheWinners() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, INDEX_B), null, null));
        createIndicesAAndB();
        String opaqueId = randomAlphaOfLength(10);
        RequestOptions options = userOptions().toBuilder().addHeader("X-Opaque-Id", opaqueId).build();

        assertRowsWithAndWithoutTheFetch(options, A_AND_B, List.of(List.of(40, "engineering"), List.of(35, "sales"), List.of(30, "sales")));

        assertBusy(() -> {
            assertThat(grantedActionsOf(GATEWAY_NODE_INDEX, opaqueId), hasItem(PROXY_ACTION_PREFIX + FETCH_ACTION));
            assertThat(grantedIndicesOf(DATA_NODE_INDEX, FETCH_ACTION, opaqueId), hasItem(INDEX_A));
            assertThat("the gateway holds no index", grantedIndicesOf(GATEWAY_NODE_INDEX, FETCH_ACTION, opaqueId), empty());
        });
    }

    /**
     * The coordinator of the remote cluster, the gateway node here, frees the contexts that no row leaving its cluster reads
     * before it answers, as an index read of the user. The querying cluster never holds them, so no free of theirs goes
     * through the gateway.
     */
    public void testTheRemoteCoordinatorFreesTheContextsOfItsLosers() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, INDEX_C), null, null));
        createIndex(INDEX_A, DATA_NODE, DOCS_OF_A);
        createIndex(INDEX_C, SLOW_NODE, DOCS_OF_C);
        String opaqueId = randomAlphaOfLength(10);
        RequestOptions options = userOptions().toBuilder().addHeader("X-Opaque-Id", opaqueId).build();

        Map<String, Object> response = query(options, A_AND_C, true);

        assertThat(rows(response), equalTo(List.of(List.of(40, "engineering"), List.of(30, "sales"))));
        assertThat(fetchedFrom(response), equalTo(List.of(DATA_NODE)));
        assertBusy(() -> assertThat(grantedIndicesOf(SLOW_NODE_INDEX, FETCH_ACTION + "/free", opaqueId), hasItem(INDEX_C)));
        assertThat(grantedActionsOf(GATEWAY_NODE_INDEX, opaqueId), not(hasItem(PROXY_ACTION_PREFIX + FETCH_ACTION + "/free")));
    }

    /**
     * DLS of the remote role on a column that only the fetch loads selects the rows in the query phase, and the fetch loads
     * the rows the query phase selected.
     */
    public void testDlsOfTheRemoteRoleSelectsTheFetchedRows() throws IOException {
        putRole(indexPrivileges(List.of(INDEX_A, INDEX_B), null, "{\"term\": {\"org\": \"sales\"}}"));
        createIndicesAAndB();

        assertRowsWithAndWithoutTheFetch(userOptions(), A_AND_B, List.of(List.of(35, "sales"), List.of(30, "sales"), List.of(15, "sales")));
    }

    /**
     * FLS of the remote role that hides {@code org} on one index hides it in the rows the fetch loads from that index only.
     */
    public void testFlsOfTheRemoteRoleHidesAFetchedColumnOnTheIndexItCovers() throws IOException {
        putRole(
            indexPrivileges(List.of(INDEX_A), List.of("value", "org"), null),
            indexPrivileges(List.of(INDEX_B), List.of("value"), null)
        );
        createIndicesAAndB();

        assertRowsWithAndWithoutTheFetch(
            userOptions(),
            A_AND_B,
            List.of(List.of(40, "engineering"), Arrays.asList(35, null), List.of(30, "sales"))
        );
    }

    /**
     * FLS that starts hiding {@code org} between the phases hides it in the fetched rows: the remote data node reads with the
     * roles of the user at fetch time.
     */
    public void testFlsThatChangesBetweenThePhasesHidesTheFetchedColumn() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));

        Map<String, Object> response = queryWhileTheRoleChanges(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), List.of("value"), null));

        assertThat(rows(response), equalTo(List.of(Arrays.asList(40, null), Arrays.asList(30, null))));
        assertThat(fetchedFrom(response), equalTo(List.of(DATA_NODE)));
    }

    /**
     * DLS that starts hiding the {@code engineering} documents between the phases doesn't hide the winners the query phase
     * selected: the remote data node reads the documents by id, like the fetch phase of {@code _search}.
     */
    public void testDlsThatChangesBetweenThePhasesKeepsTheRowsOfTheQueryPhase() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));

        Map<String, Object> response = queryWhileTheRoleChanges(
            indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, "{\"term\": {\"org\": \"sales\"}}")
        );

        assertThat(rows(response), equalTo(List.of(List.of(40, "engineering"), List.of(30, "sales"))));
        assertThat(fetchedFrom(response), equalTo(List.of(DATA_NODE)));
    }

    /**
     * A user who loses read on the index of the winners between the phases fails the query, even with
     * {@code allow_partial_results}: the remote data node denies the fetch. It denies the keep-alives and the free of those
     * contexts too, so they stay open until their keep-alive passes.
     */
    public void testReadRevokedBetweenThePhasesFailsTheQuery() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        // longer than the stall, so the data node still holds the contexts when it denies the fetch
        updateFulfillingClusterSettings(Settings.builder().put("search.default_keep_alive", (STALL_SECONDS * 2) + "s").build());
        updateClusterSettings(Settings.builder().put("search.default_keep_alive", (STALL_SECONDS * 2) + "s").build());
        try {
            ResponseException e = expectThrows(
                ResponseException.class,
                () -> queryWhileTheRoleChanges(indexPrivileges(List.of(SLOW_INDEX), null, null))
            );

            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString(FETCH_ACTION));
            assertBusy(() -> assertThat(openContexts(DATA_NODE), equalTo(0)), STALL_SECONDS * 6, TimeUnit.SECONDS);
        } finally {
            updateFulfillingClusterSettings(Settings.builder().putNull("search.default_keep_alive").build());
            updateClusterSettings(Settings.builder().putNull("search.default_keep_alive").build());
        }
    }

    /**
     * Read on {@code names}, with FLS that grants {@code grantedFields} and DLS {@code query} when they aren't {@code null}.
     */
    protected static String indexPrivileges(List<String> names, @Nullable List<String> grantedFields, @Nullable String query) {
        StringBuilder privileges = new StringBuilder("{\"names\": [");
        privileges.append(String.join(", ", names.stream().map(name -> "\"" + name + "\"").toList()));
        privileges.append("], \"privileges\": [\"read\"]");
        if (grantedFields != null) {
            privileges.append(", \"field_security\": {\"grant\": [");
            privileges.append(String.join(", ", grantedFields.stream().map(field -> "\"" + field + "\"").toList()));
            privileges.append("]}");
        }
        if (query != null) {
            privileges.append(", \"query\": ").append(query);
        }
        return privileges.append("}").toString();
    }

    /**
     * Creates {@link #INDEX_A} and {@link #SLOW_INDEX}, starts {@link #A_AND_SLOW} with the query phase of {@link #SLOW_INDEX}
     * stalled, changes {@link #ROLE} to {@code indexPrivilegesAfter} once the data node holds the contexts of the winners,
     * and returns the response.
     */
    private Map<String, Object> queryWhileTheRoleChanges(String indexPrivilegesAfter) throws Exception {
        createIndicesAAndSlow();
        String id = startAsyncQueryWithStall(userOptions(), A_AND_SLOW);
        assertBusy(() -> assertThat(openContexts(DATA_NODE), greaterThan(0)));
        putRole(indexPrivilegesAfter);
        return awaitAsyncQuery(userOptions(), id);
    }

    private void createIndicesAAndB() throws IOException {
        createIndex(INDEX_A, DATA_NODE, DOCS_OF_A);
        createIndex(INDEX_B, DATA_NODE, DOCS_OF_B);
    }

    private void createIndicesAAndSlow() throws IOException {
        createIndex(INDEX_A, DATA_NODE, DOCS_OF_A);
        createSlowIndex();
    }

    /**
     * {@link #SLOW_INDEX} on the querying cluster, one shard.
     */
    private void createSlowIndex() throws IOException {
        Request create = new Request("PUT", "/" + SLOW_INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0},
              "mappings": {"properties": {"value": {"type": "long"}, "org": {"type": "keyword"}}}
            }""");
        assertOK(adminClient().performRequest(create));
        StringBuilder bulk = new StringBuilder();
        for (List<Object> doc : DOCS_OF_SLOW) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(Strings.format("{\"value\": %s, \"org\": \"%s\"}\n", doc.get(0), doc.get(1)));
        }
        Request request = new Request("POST", "/" + SLOW_INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(ObjectPath.createFromResponse(adminClient().performRequest(request)).evaluate("errors"), equalTo(false));
    }

    /**
     * One shard on {@code node} of the fulfilling cluster, with a document for each {@code value} and {@code org} of
     * {@code docs}.
     */
    private static void createIndex(String index, String node, List<List<Object>> docs) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0, "index.routing.allocation.require._name": "%s"},
              "mappings": {"properties": {"value": {"type": "long"}, "org": {"type": "keyword"}}}
            }""", node));
        assertOK(performRequestAgainstFulfillingCluster(create));
        StringBuilder bulk = new StringBuilder();
        for (List<Object> doc : docs) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(Strings.format("{\"value\": %s, \"org\": \"%s\"}\n", doc.get(0), doc.get(1)));
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = performRequestAgainstFulfillingCluster(request);
        assertThat(ObjectPath.createFromResponse(response).evaluate("errors"), equalTo(false));
    }

    /**
     * Runs {@code query} with and without the fetch, and checks that both return {@code expected} and that only the first
     * fetched, from the data node of the fulfilling cluster.
     */
    private void assertRowsWithAndWithoutTheFetch(RequestOptions options, String query, List<List<Object>> expected) throws IOException {
        Map<String, Object> withTheFetch = query(options, query, true);
        assertThat(rows(withTheFetch), equalTo(expected));
        assertThat(fetchedFrom(withTheFetch), equalTo(List.of(DATA_NODE)));
        Map<String, Object> withoutTheFetch = query(options, query, false);
        assertThat(rows(withoutTheFetch), equalTo(expected));
        assertThat(fetchedFrom(withoutTheFetch), empty());
    }

    private Map<String, Object> query(RequestOptions options, String query, boolean fetchPhase) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("allow_partial_results", "true");
        request.setJsonEntity(body(query, fetchPhase, false));
        request.setOptions(options);
        return entityAsMap(client().performRequest(request));
    }

    private String startAsyncQueryWithStall(RequestOptions options, String query) throws IOException {
        Request request = new Request("POST", "/_query/async");
        request.addParameter("allow_partial_results", "true");
        request.setJsonEntity(body(query, true, true));
        request.setOptions(options);
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat("the stall keeps the query running", response.get("is_running"), equalTo(true));
        return (String) response.get("id");
    }

    private Map<String, Object> awaitAsyncQuery(RequestOptions options, String id) throws IOException {
        Request request = new Request("GET", "/_query/async/" + id);
        request.addParameter("wait_for_completion_timeout", (STALL_SECONDS * 6) + "s");
        request.setOptions(options);
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("is_running"), equalTo(false));
        return response;
    }

    /**
     * The body of a query with the {@code fetch_phase} pragma set to {@code fetchPhase} and the profile on. A stalled query
     * runs asynchronously and stalls the query phase of {@link #SLOW_INDEX} for {@link #STALL_SECONDS}.
     */
    private static String body(String query, boolean fetchPhase, boolean stall) throws IOException {
        XContentBuilder json = JsonXContent.contentBuilder().startObject();
        json.field("query", query);
        json.field("profile", true);
        json.field("accept_pragma_risks", true);
        json.startObject("pragma").field("fetch_phase", fetchPhase).endObject();
        if (stall) {
            json.field("wait_for_completion_timeout", "0s");
            json.field("keep_on_completion", false);
            json.startObject("filter").startObject("error_query").startArray("indices");
            json.startObject().field("name", SLOW_INDEX).field("error_type", "none").field("stall_time_seconds", STALL_SECONDS).endObject();
            json.endArray().endObject().endObject();
        }
        return Strings.toString(json.endObject());
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> response) {
        return (List<List<Object>>) response.get("values");
    }

    /**
     * The nodes the coordinator sent a fetch to, from the profile.
     */
    private static List<String> fetchedFrom(Map<String, Object> response) {
        List<String> nodes = new ArrayList<>();
        for (Object driver : (List<?>) ((Map<?, ?>) response.get("profile")).get("drivers")) {
            for (Object operator : (List<?>) ((Map<?, ?>) driver).get("operators")) {
                if (String.valueOf(((Map<?, ?>) operator).get("operator")).startsWith("FetchOperator")) {
                    for (Object request : (List<?>) ((Map<?, ?>) ((Map<?, ?>) operator).get("status")).get("requests")) {
                        nodes.add((String) ((Map<?, ?>) request).get("node"));
                    }
                }
            }
        }
        return nodes;
    }

    private static int openContexts(String node) throws IOException {
        Response response = performRequestAgainstFulfillingCluster(new Request("GET", "/_nodes/" + node + "/stats/indices/search"));
        Map<String, Object> nodes = ObjectPath.createFromResponse(response).evaluate("nodes");
        assertThat("the node " + node + " is in the fulfilling cluster", nodes.size(), not(equalTo(0)));
        int open = 0;
        for (Object stats : nodes.values()) {
            open += (Integer) ObjectPath.evaluate(stats, "indices.search.open_contexts");
        }
        return open;
    }

    private static void updateFulfillingClusterSettings(Settings settings) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": " + Strings.toString(settings) + "}");
        assertOK(performRequestAgainstFulfillingCluster(request));
    }

    /**
     * The actions of every {@code access_granted} event of {@link #FETCH_USER} in the audit log of the node of the fulfilling
     * cluster at {@code nodeIndex}, for the requests of the query with {@code opaqueId}.
     */
    private static List<String> grantedActionsOf(int nodeIndex, String opaqueId) throws IOException {
        List<String> actions = new ArrayList<>();
        for (Map<String, Object> event : grantedEventsOf(nodeIndex, opaqueId)) {
            actions.add((String) event.get("action"));
        }
        return actions;
    }

    /**
     * The indices of every {@code access_granted} event for {@code action} of {@link #FETCH_USER} in the audit log of the node
     * of the fulfilling cluster at {@code nodeIndex}, for the requests of the query with {@code opaqueId}.
     */
    private static List<String> grantedIndicesOf(int nodeIndex, String action, String opaqueId) throws IOException {
        List<String> indices = new ArrayList<>();
        for (Map<String, Object> event : grantedEventsOf(nodeIndex, opaqueId)) {
            if (action.equals(event.get("action")) && event.get("indices") != null) {
                for (Object index : (List<?>) event.get("indices")) {
                    indices.add((String) index);
                }
            }
        }
        return indices;
    }

    private static List<Map<String, Object>> grantedEventsOf(int nodeIndex, String opaqueId) throws IOException {
        List<Map<String, Object>> events = new ArrayList<>();
        try (InputStream log = fulfillingCluster.getNodeLog(nodeIndex, LogType.AUDIT)) {
            for (String line : Streams.readAllLines(log)) {
                Map<String, Object> event = XContentHelper.convertToMap(XContentType.JSON.xContent(), line, false);
                if ("access_granted".equals(event.get("event.action"))
                    && FETCH_USER.equals(querierOf(event))
                    && opaqueId.equals(event.get("opaque_id"))) {
                    events.add(event);
                }
            }
        }
        return events;
    }

    /**
     * The user of the querying cluster behind an audit event. Under the API key model the fulfilling cluster authenticates
     * the cross-cluster API key and records that user inside the {@code cross_cluster_access} field.
     */
    private static String querierOf(Map<String, Object> event) {
        if (event.get("cross_cluster_access") instanceof Map<?, ?> crossClusterAccess) {
            return (String) crossClusterAccess.get("user.name");
        }
        return (String) event.get("user.name");
    }

    private static RequestOptions userOptions() {
        return RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(FETCH_USER, PASS)).build();
    }
}
