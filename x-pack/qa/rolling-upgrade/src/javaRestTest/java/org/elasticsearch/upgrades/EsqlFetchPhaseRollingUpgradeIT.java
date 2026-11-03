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
 * A query that the fetch phase plans, at every stage of a rolling upgrade, with the {@code fetch_phase} pragma on. The rows
 * stay the same throughout. While an older node is in the cluster nothing is fetched: an older coordinator doesn't know the
 * pragma, and an upgraded one plans eagerly because the older nodes can't read the plans of the fetch phase. Once every
 * node is upgraded and has the fetch phase, the query fetches.
 */
public class EsqlFetchPhaseRollingUpgradeIT extends AbstractXpackRollingUpgradeTestCase {
    private static final String INDEX = "esql_fetch_phase_upgrade";
    private static final int DOCS = 100;
    private static final int LIMIT = 10;
    /** {@code ts} is unique, so every stage returns the same rows in the same order. */
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT " + LIMIT + " | KEEP ts, payload";

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
        Map<String, Object> response = query();
        assertThat(response.get("values"), equalTo(expectedRows()));
        int expectedFetchOperators = everyNodeHasTheFetchPhase() ? 1 : 0;
        assertThat("the fetch operators of the query", fetchOperators(response), equalTo(expectedFetchOperators));
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

    private static Map<String, Object> query() throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("query", QUERY);
        body.field("profile", true);
        body.field("accept_pragma_risks", true);
        body.startObject("pragma").field("fetch_phase", true).endObject();
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
