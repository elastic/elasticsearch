/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Two queries that the fetch phase plans, at every stage of a rolling upgrade: one with the {@code fetch_phase} pragma on,
 * and one that the cluster setting fetches without the pragma, since the setting is on by default in a build with the fetch
 * phase. The rows stay the same throughout. While an older node is in the cluster nothing is fetched: an older coordinator
 * doesn't know the fetch phase, and an upgraded one plans eagerly because the older nodes can't read its plans. Once every
 * node is upgraded and has the fetch phase, both queries fetch.
 */
public class EsqlFetchPhaseRollingUpgradeIT extends AbstractXpackRollingUpgradeTestCase {
    private static final String INDEX = "esql_fetch_phase_upgrade";
    private static final int DOCS = 100;
    private static final int LIMIT = 10;
    /** {@code ts} is unique, so every stage returns the same rows in the same order. */
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";
    /** The setting fetches a {@code TopN} over more than one shard that defers {@code _source}. */
    private static final String SOURCE_QUERY = "FROM "
        + INDEX
        + " METADATA _source | SORT ts DESC | LIMIT "
        + LIMIT
        + " | KEEP ts, _source";

    @ClassRule
    public static ElasticsearchCluster cluster = buildCluster();

    public EsqlFetchPhaseRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testTheRowsStayTheSameThroughTheUpgrade() throws IOException {
        if (isOldCluster()) {
            createIndex();
        }
        int expectedFetchOperators = everyNodeHasTheFetchPhase() ? 1 : 0;

        Map<String, Object> withThePragma = query(QUERY, true);
        assertThat(withThePragma.get("values"), equalTo(expectedRows()));
        assertThat("the fetch operators of the query with the pragma", fetchOperators(withThePragma), equalTo(expectedFetchOperators));

        Map<String, Object> byDefault = query(SOURCE_QUERY, null);
        assertThat(byDefault.get("values"), equalTo(expectedSourceRows()));
        assertThat("the fetch operators of the query without the pragma", fetchOperators(byDefault), equalTo(expectedFetchOperators));
    }

    private void createIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 3, "number_of_replicas": 0},
              "mappings": {"properties": {"ts": {"type": "long"}, "payload": {"type": "keyword"}}}
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int ts = 0; ts < DOCS; ts++) {
            bulk.append("{\"index\":{}}\n").append(Strings.format("{\"ts\": %s, \"payload\": \"payload-%s\"}\n", ts, ts));
        }
        Request request = new Request("POST", "/" + INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(ObjectPath.createFromResponse(client().performRequest(request)).evaluate("errors"), equalTo(false));
    }

    private static List<List<Object>> expectedRows() {
        List<List<Object>> rows = new ArrayList<>();
        for (int ts = DOCS - 1; ts >= DOCS - LIMIT; ts--) {
            rows.add(List.of(ts, "payload-" + ts));
        }
        return rows;
    }

    private static List<List<Object>> expectedSourceRows() {
        List<List<Object>> rows = new ArrayList<>();
        for (int ts = DOCS - 1; ts >= DOCS - LIMIT; ts--) {
            rows.add(List.of(ts, Map.of("ts", ts, "payload", "payload-" + ts)));
        }
        return rows;
    }

    /**
     * @param fetchPhasePragma the {@code fetch_phase} pragma, {@code null} to follow the cluster setting
     */
    private static Map<String, Object> query(String query, @Nullable Boolean fetchPhasePragma) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", query);
        body.field("profile", true);
        if (fetchPhasePragma != null) {
            body.field("accept_pragma_risks", true);
            body.startObject("pragma").field("fetch_phase", fetchPhasePragma).endObject();
        }
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(body.endObject()));
        return entityAsMap(client().performRequest(request));
    }

    /**
     * Whether every node has the fetch phase. Only snapshot builds of a version that carries it do.
     */
    private static boolean everyNodeHasTheFetchPhase() throws IOException {
        Request request = new Request("GET", "_capabilities");
        request.addParameter("method", "POST");
        request.addParameter("path", "/_query");
        request.addParameter("capabilities", "fetch_phase");
        return Boolean.TRUE.equals(entityAsMap(client().performRequest(request)).get("supported"));
    }

    private static int fetchOperators(Map<String, Object> response) {
        int fetchOperators = 0;
        for (Object driver : (List<?>) ((Map<?, ?>) response.get("profile")).get("drivers")) {
            for (Object operator : (List<?>) ((Map<?, ?>) driver).get("operators")) {
                if (String.valueOf(((Map<?, ?>) operator).get("operator")).startsWith("FetchOperator")) {
                    fetchOperators++;
                }
            }
        }
        return fetchOperators;
    }
}
