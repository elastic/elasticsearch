/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

/**
 * The fetch phase with security, on a cluster where the fetch crosses the network. The REST client talks to a coordinating
 * only node, and the indices live on the other nodes. Every query runs with the {@code fetch_phase} pragma and checks in its
 * profile that the coordinator fetched from the {@link #DATA} node, so the fetch went through the transport authorization of
 * that node. Every query allows partial results, so a query that fails here fails even then.
 * <p>
 * The data node authorizes each fetch, keep-alive and free like an index read of the query, with the roles of the user at
 * that time. FLS applies again at the fetch. DLS applies when the query phase selects the documents and is not checked again
 * for the documents the fetch reads, like {@code _search}. A user who lost read on an index between the phases fails the
 * query.
 * <p>
 * The tests that change a role between the phases stall the query phase of {@link #SLOW_INDEX} on the {@link #SLOW} node
 * with {@code error_query}. The data node holds the contexts of its winners meanwhile, and the coordinator fetches once the
 * stalled shard answers.
 */
public class EsqlFetchPhaseSecurityIT extends ESRestTestCase {
    private static final String COORDINATOR = "coordinator";
    private static final String DATA = "data";
    private static final String SLOW = "slow";
    /** The index of a node for {@code cluster.getNodeLog}, in the order the cluster declares them. */
    private static final int DATA_NODE_INDEX = 1;
    private static final int SLOW_NODE_INDEX = 2;

    private static final String PASSWORD = "x-pack-test-password";
    private static final String ADMIN = "fetch_admin";
    /** The user of the queries. Each test gives {@link #ROLE} the privileges it needs. */
    private static final String USER = "fetch_user";
    private static final String ROLE = "fetch_role";

    /** On {@link #DATA}. */
    private static final String INDEX_A = "fetch-a";
    private static final List<List<Object>> DOCS_OF_A = List.of(
        List.of(10, "sales"),
        List.of(20, "engineering"),
        List.of(30, "sales"),
        List.of(40, "engineering")
    );
    /** On {@link #DATA}, with values between those of {@link #INDEX_A}. */
    private static final String INDEX_B = "fetch-b";
    private static final List<List<Object>> DOCS_OF_B = List.of(List.of(15, "sales"), List.of(25, "engineering"), List.of(35, "sales"));
    /** On {@link #SLOW}, with values below all others, so its rows lose the cut. */
    private static final String SLOW_INDEX = "fetch-slow";
    private static final List<List<Object>> DOCS_OF_SLOW = List.of(List.of(1, "sales"), List.of(2, "engineering"));
    /** Long enough to change a role, short enough for a test. */
    private static final int STALL_SECONDS = 5;

    /** The fetch, and the prefix of its keep-alive and free actions. */
    private static final String FETCH_ACTION = "indices:data/read/esql/fetch";

    private static final String A_AND_B = "FROM " + INDEX_A + "," + INDEX_B + " | SORT value DESC | LIMIT 3 | KEEP value, org";
    private static final String A_AND_SLOW = "FROM " + INDEX_A + "," + SLOW_INDEX + " | SORT value DESC | LIMIT 2 | KEEP value, org";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .withNode(node -> node.name(COORDINATOR).setting("node.roles", "[]"))
        .withNode(node -> node.name(DATA).setting("node.roles", "[master, data]"))
        .withNode(node -> node.name(SLOW).setting("node.roles", "[data]"))
        .module("test-error-query")
        // frees an expired context within a second, for the contexts that a user who lost read can't free
        .setting("search.keep_alive_interval", "1s")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.audit.logfile.events.include", "[\"access_granted\", \"access_denied\"]")
        .user(ADMIN, PASSWORD, "superuser", false)
        .user(USER, PASSWORD, ROLE, false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(0);
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", basicAuth(ADMIN)).build();
    }

    @Before
    public void checkTheFetchPhaseIsAvailable() throws IOException {
        assumeTrue("the cluster runs the fetch phase", hasCapabilities(adminClient(), List.of("fetch_phase")));
    }

    @After
    public void checkNoContextIsLeft() throws Exception {
        assertBusy(() -> {
            assertThat("open contexts on " + DATA, openContexts(DATA), equalTo(0));
            assertThat("open contexts on " + SLOW, openContexts(SLOW), equalTo(0));
        });
    }

    /**
     * DLS on a column that only the fetch loads selects the rows in the query phase. The fetch loads the rows the query phase
     * selected, so no hidden document comes back.
     */
    public void testDlsOfTheQueryPhaseSelectsTheFetchedRows() throws IOException {
        putRole(indexPrivileges(List.of(INDEX_A, INDEX_B), null, "{\"term\": {\"org\": \"sales\"}}"));
        createIndicesAAndB();
        assertRowsWithAndWithoutTheFetch(userOptions(), A_AND_B, List.of(List.of(35, "sales"), List.of(30, "sales"), List.of(15, "sales")));
    }

    /**
     * FLS that hides {@code org} on one index hides it in the rows the fetch loads from that index, and only there.
     */
    public void testFlsHidesAFetchedColumnOnTheIndexItCovers() throws IOException {
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
     * A user who runs as {@link #USER} fetches with the roles of {@link #USER}, not with its own.
     */
    public void testRunAsFetchesWithTheRolesOfTheRunAsUser() throws IOException {
        putRole(indexPrivileges(List.of(INDEX_A, INDEX_B), null, "{\"term\": {\"org\": \"sales\"}}"));
        createIndicesAAndB();
        RequestOptions runAs = RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", USER).build();
        assertRowsWithAndWithoutTheFetch(runAs, A_AND_B, List.of(List.of(35, "sales"), List.of(30, "sales"), List.of(15, "sales")));
    }

    /**
     * An API key fetches with the role descriptors of the key, which hide {@code org} on {@link #INDEX_B} for an owner who may
     * see everything.
     */
    public void testApiKeyFetchesWithTheRolesOfTheKey() throws IOException {
        createIndicesAAndB();
        Request createKey = new Request("POST", "/_security/api_key");
        createKey.setJsonEntity(
            Strings.format(
                """
                    {"name": "fetch_key", "role_descriptors": {"fetch_key_role": {"indices": [%s, %s]}}}""",
                indexPrivileges(List.of(INDEX_A), List.of("value", "org"), null),
                indexPrivileges(List.of(INDEX_B), List.of("value"), null)
            )
        );
        String encoded = ObjectPath.createFromResponse(client().performRequest(createKey)).evaluate("encoded");
        RequestOptions apiKey = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded).build();
        assertRowsWithAndWithoutTheFetch(
            apiKey,
            A_AND_B,
            List.of(List.of(40, "engineering"), Arrays.asList(35, null), List.of(30, "sales"))
        );
    }

    /**
     * A pattern that matches an index the user may not read resolves without it, so the fetch only loads the rows of the
     * other index.
     */
    public void testAPatternSkipsTheIndexTheUserMayNotRead() throws IOException {
        putRole(indexPrivileges(List.of(INDEX_A), null, null));
        createIndicesAAndB();
        String query = "FROM fetch-* | SORT value DESC | LIMIT 2 | KEEP value, org";
        assertRowsWithAndWithoutTheFetch(userOptions(), query, List.of(List.of(40, "engineering"), List.of(30, "sales")));
    }

    /**
     * Naming an index the user may not read fails before the query phase, with the same 403 as a query without the fetch.
     */
    public void testNamingAnIndexTheUserMayNotReadFailsLikeWithoutTheFetch() throws IOException {
        putRole(indexPrivileges(List.of(INDEX_A), null, null));
        createIndicesAAndB();
        ResponseException withTheFetch = expectThrows(ResponseException.class, () -> query(userOptions(), A_AND_B, true));
        ResponseException withoutTheFetch = expectThrows(ResponseException.class, () -> query(userOptions(), A_AND_B, false));
        assertThat(withTheFetch.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(errorType(withTheFetch), equalTo("security_exception"));
        assertThat(errorType(withTheFetch), equalTo(errorType(withoutTheFetch)));
        assertThat(withTheFetch.getMessage(), containsString(INDEX_B));
    }

    /**
     * FLS that starts hiding {@code org} between the phases hides it in the fetched rows: the fetch reads with the roles of
     * the user at fetch time.
     */
    public void testFlsThatChangesBetweenThePhasesHidesTheFetchedColumn() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        Map<String, Object> response = queryWhileTheRoleChanges(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), List.of("value"), null));
        assertThat(rows(response), equalTo(List.of(Arrays.asList(40, null), Arrays.asList(30, null))));
        assertThat(fetchedFrom(response), equalTo(List.of(DATA)));
    }

    /**
     * DLS that starts hiding the {@code engineering} documents between the phases doesn't hide the winners the query phase
     * selected: the fetch reads the documents by id and doesn't check DLS again, like the fetch phase of {@code _search}.
     */
    public void testDlsThatChangesBetweenThePhasesKeepsTheRowsOfTheQueryPhase() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        Map<String, Object> response = queryWhileTheRoleChanges(
            indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, "{\"term\": {\"org\": \"sales\"}}")
        );
        assertThat(rows(response), equalTo(List.of(List.of(40, "engineering"), List.of(30, "sales"))));
        assertThat(fetchedFrom(response), equalTo(List.of(DATA)));
    }

    /**
     * A user who loses read on the index of the winners between the phases fails the query with 403, even with
     * {@code allow_partial_results}: the data node denies the fetch. It denies the keep-alives and the free of the contexts
     * of that index too, so they stay open until their keep-alive passes.
     */
    public void testReadRevokedBetweenThePhasesFailsTheQuery() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        // longer than the stall, so the data node still holds the contexts when it denies the fetch
        updateClusterSettings(Settings.builder().put("search.default_keep_alive", (STALL_SECONDS * 2) + "s").build());
        try {
            ResponseException e = expectThrows(
                ResponseException.class,
                () -> queryWhileTheRoleChanges(indexPrivileges(List.of(SLOW_INDEX), null, null))
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(errorType(e), equalTo("security_exception"));
            assertThat(e.getMessage(), containsString(FETCH_ACTION));
            assertThat("the contexts the user may no longer free", openContexts(DATA), greaterThan(0));
            assertBusy(() -> assertThat(openContexts(DATA), equalTo(0)), STALL_SECONDS * 6, TimeUnit.SECONDS);
        } finally {
            updateClusterSettings(Settings.builder().putNull("search.default_keep_alive").build());
        }
    }

    /**
     * The data node audits the fetch of the winners as an index read of the user, and the node of the losers audits the
     * free of their contexts the same way.
     */
    public void testTheFetchAndTheFreeAreAuditedAsTheUser() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        createIndicesAAndSlow();
        String opaqueId = randomAlphaOfLength(10);
        Map<String, Object> response = query(userOptions().toBuilder().addHeader("X-Opaque-Id", opaqueId).build(), A_AND_SLOW, true);
        assertThat(fetchedFrom(response), equalTo(List.of(DATA)));
        assertBusy(() -> {
            assertThat(grantedIndicesOf(DATA_NODE_INDEX, FETCH_ACTION, opaqueId), hasItem(INDEX_A));
            assertThat(grantedIndicesOf(SLOW_NODE_INDEX, FETCH_ACTION + "/free", opaqueId), hasItem(SLOW_INDEX));
        });
    }

    /**
     * While the query waits for a stalled shard, the coordinator keeps the contexts of the winners alive, and the data node
     * audits each keep-alive as an index read of the user.
     */
    public void testTheKeepAlivesAreAuditedAsTheUser() throws Exception {
        putRole(indexPrivileges(List.of(INDEX_A, SLOW_INDEX), null, null));
        createIndicesAAndSlow();
        // the coordinator refreshes the contexts every third of the keep-alive, so several times during the stall
        updateClusterSettings(Settings.builder().put("search.default_keep_alive", "1s").build());
        try {
            String opaqueId = randomAlphaOfLength(10);
            RequestOptions options = userOptions().toBuilder().addHeader("X-Opaque-Id", opaqueId).build();
            String id = startAsyncQueryWithStall(options, A_AND_SLOW);
            Map<String, Object> response = awaitAsyncQuery(options, id);
            assertThat(fetchedFrom(response), equalTo(List.of(DATA)));
            assertBusy(() -> assertThat(grantedIndicesOf(DATA_NODE_INDEX, FETCH_ACTION + "/keep_alive", opaqueId), hasItem(INDEX_A)));
        } finally {
            updateClusterSettings(Settings.builder().putNull("search.default_keep_alive").build());
        }
    }

    /**
     * Creates {@link #INDEX_A} and {@link #SLOW_INDEX}, starts {@link #A_AND_SLOW} with the query phase of {@link #SLOW_INDEX}
     * stalled, changes {@link #ROLE} to {@code indexPrivilegesAfter} once the data node holds the contexts of the winners,
     * and returns the response.
     */
    private Map<String, Object> queryWhileTheRoleChanges(String indexPrivilegesAfter) throws Exception {
        createIndicesAAndSlow();
        String id = startAsyncQueryWithStall(userOptions(), A_AND_SLOW);
        assertBusy(() -> assertThat(openContexts(DATA), greaterThan(0)));
        putRole(indexPrivilegesAfter);
        return awaitAsyncQuery(userOptions(), id);
    }

    private void createIndicesAAndB() throws IOException {
        createIndex(INDEX_A, DATA, DOCS_OF_A);
        createIndex(INDEX_B, DATA, DOCS_OF_B);
    }

    private void createIndicesAAndSlow() throws IOException {
        createIndex(INDEX_A, DATA, DOCS_OF_A);
        createIndex(SLOW_INDEX, SLOW, DOCS_OF_SLOW);
    }

    /**
     * One shard on {@code node}, with a document for each {@code value} and {@code org} of {@code docs}.
     */
    private void createIndex(String index, String node, List<List<Object>> docs) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0, "index.routing.allocation.require._name": "%s"},
              "mappings": {"properties": {"value": {"type": "long"}, "org": {"type": "keyword"}}}
            }""", node));
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (List<Object> doc : docs) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(Strings.format("{\"value\": %s, \"org\": \"%s\"}\n", doc.get(0), doc.get(1)));
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(ObjectPath.createFromResponse(client().performRequest(request)).evaluate("errors"), equalTo(false));
    }

    private void putRole(String... indexPrivileges) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + ROLE);
        request.setJsonEntity("{\"indices\": [" + String.join(", ", indexPrivileges) + "]}");
        client().performRequest(request);
    }

    /**
     * Read on {@code names}, with FLS that grants {@code grantedFields} and DLS {@code query} when they aren't {@code null}.
     */
    private static String indexPrivileges(List<String> names, @Nullable List<String> grantedFields, @Nullable String query) {
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
     * Runs {@code query} with and without the fetch, and checks that both return {@code expected} and that only the first
     * fetched, from the data node.
     */
    private void assertRowsWithAndWithoutTheFetch(RequestOptions options, String query, List<List<Object>> expected) throws IOException {
        Map<String, Object> withTheFetch = query(options, query, true);
        assertThat(rows(withTheFetch), equalTo(expected));
        assertThat(fetchedFrom(withTheFetch), equalTo(List.of(DATA)));
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
                Map<?, ?> status = (Map<?, ?>) ((Map<?, ?>) operator).get("status");
                if (String.valueOf(((Map<?, ?>) operator).get("operator")).startsWith("FetchOperator")) {
                    for (Object request : (List<?>) status.get("requests")) {
                        nodes.add((String) ((Map<?, ?>) request).get("node"));
                    }
                }
            }
        }
        return nodes;
    }

    private static String errorType(ResponseException e) throws IOException {
        return ObjectPath.createFromResponse(e.getResponse()).evaluate("error.type");
    }

    private int openContexts(String node) throws IOException {
        Response response = client().performRequest(new Request("GET", "/_nodes/" + node + "/stats/indices/search"));
        Map<String, Object> nodes = ObjectPath.createFromResponse(response).evaluate("nodes");
        int open = 0;
        for (Object stats : nodes.values()) {
            open += (Integer) ObjectPath.evaluate(stats, "indices.search.open_contexts");
        }
        return open;
    }

    /**
     * The indices of every {@code access_granted} event for {@code action} of {@link #USER} in the audit log of the node at
     * {@code nodeIndex}, for the requests of the query with {@code opaqueId}.
     */
    private static List<String> grantedIndicesOf(int nodeIndex, String action, String opaqueId) throws IOException {
        List<String> indices = new ArrayList<>();
        try (InputStream log = cluster.getNodeLog(nodeIndex, LogType.AUDIT)) {
            for (String line : Streams.readAllLines(log)) {
                Map<String, Object> event = XContentHelper.convertToMap(XContentType.JSON.xContent(), line, false);
                if ("access_granted".equals(event.get("event.action"))
                    && action.equals(event.get("action"))
                    && USER.equals(event.get("user.name"))
                    && opaqueId.equals(event.get("opaque_id"))) {
                    for (Object index : (List<?>) event.get("indices")) {
                        indices.add((String) index);
                    }
                }
            }
        }
        return indices;
    }

    private static RequestOptions userOptions() {
        return RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(USER)).build();
    }

    private static String basicAuth(String user) {
        return basicAuthHeaderValue(user, new SecureString(PASSWORD.toCharArray()));
    }
}
