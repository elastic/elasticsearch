/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Verifies that the ES-side early-termination gate works for keyword sorts on
 * {@code logsdb_columnar} indices after the fix in #158928.
 *
 * <p>In {@code logsdb_columnar} mode the keyword index-sort field for {@code host.name} is stored
 * as a {@code MultiValuedBinaryDocValuesSortField} (a subclass of Lucene's {@code BinarySortField}).
 * The query-side sort field wraps a {@code BytesRefFieldComparatorSource} (type
 * {@code SortField.Type.CUSTOM}). Before the fix, {@code Lucene.canEarlyTerminate()} delegated to
 * {@code SortField.equals()}, which compared comparator sources and returned false because the two
 * field types differ, even though field name, direction, and missing-value sentinel all match.
 *
 * <p>The observable consequence: {@code QueryPhase} skips injecting a
 * {@code SearchAfterSortedDocQuery} for {@code search_after} requests, so every page re-scans
 * the full segment instead of pruning already-seen documents.
 */
public class ColumnarKeywordEarlyTerminationIT extends ESRestTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .user(USER, PASS)
        .keystore("bootstrap.password", "x-pack-test-password")
        .setting("xpack.license.self_generated.type", "trial")
        .nodes(1)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Covers the ES-side gate: {@code Lucene#canEarlyTerminate()} in {@code QueryPhase}.
     *
     * <p>When the gate returns true, {@code QueryPhase} injects a {@code SearchAfterSortedDocQuery}
     * filter into the Lucene query for any request that carries a {@code search_after} position.
     * The query is injected before {@code IndexSearcher.search()} so Lucene's profiler wraps it
     * alongside all other query clauses. When the gate returns false the injection is skipped, and
     * the profile shows only the original query with no {@code SearchAfterSortedDocQuery}.
     */
    public void testLogsdbColumnarKeywordSearchAfterEsGate() throws IOException {
        final String index = "test-columnar-et-es-gate";
        createLogsdbColumnarIndex(index);
        indexDocuments(index);
        forcemerge(index);

        final Map<String, Object> firstPage = searchSortedByHostName(index, 5, false, null);
        @SuppressWarnings("unchecked")
        final List<Object> firstHits = (List<Object>) ((Map<String, Object>) firstPage.get("hits")).get("hits");
        @SuppressWarnings("unchecked")
        final String searchAfterValue = (String) ((List<Object>) ((Map<String, Object>) firstHits.get(firstHits.size() - 1)).get("sort"))
            .get(0);

        final Map<String, Object> profiled = searchSortedByHostName(index, 5, true, searchAfterValue);

        assertTrue(
            "SearchAfterSortedDocQuery must appear in the query profile when the ES-side gate works",
            profileContainsQueryType(profiled, "SearchAfterSortedDocQuery")
        );
    }

    private void createLogsdbColumnarIndex(String index) throws IOException {
        final Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "settings": {
                "index.mode": "logsdb_columnar",
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0
              },
              "mappings": {
                "properties": {
                  "host.name": { "type": "keyword" },
                  "@timestamp": { "type": "date" }
                }
              }
            }
            """);
        assertOK(client().performRequest(create));
    }

    private void indexDocuments(String index) throws IOException {
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append(
                String.format(Locale.ROOT, "{\"host.name\":\"host-%02d\",\"@timestamp\":\"2024-01-01T00:%02d:00Z\"}\n", i % 12, i % 60)
            );
        }
        final Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        assertOK(client().performRequest(bulkRequest));
    }

    private void forcemerge(String index) throws IOException {
        final Request merge = new Request("POST", "/" + index + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(merge));
    }

    private Map<String, Object> searchSortedByHostName(String index, int size, boolean profile, String searchAfterValue)
        throws IOException {
        final Request search = new Request("GET", "/" + index + "/_search");
        final StringBuilder body = new StringBuilder();
        body.append(String.format(Locale.ROOT, "{\"size\":%d,\"sort\":[{\"host.name\":{\"order\":\"asc\"}}]", size));
        if (profile) {
            body.append(",\"profile\":true");
        }
        if (searchAfterValue != null) {
            body.append(String.format(Locale.ROOT, ",\"search_after\":[\"%s\"]", searchAfterValue));
        }
        body.append("}");
        search.setJsonEntity(body.toString());
        final Response response = client().performRequest(search);
        assertOK(response);
        return responseAsMap(response);
    }

    @SuppressWarnings("unchecked")
    private static boolean profileContainsQueryType(Map<String, Object> response, String queryType) {
        final Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        if (profile == null) {
            return false;
        }
        for (Object shard : (List<Object>) profile.get("shards")) {
            for (Object search : (List<Object>) ((Map<String, Object>) shard).get("searches")) {
                final List<Object> queryNodes = (List<Object>) ((Map<String, Object>) search).get("query");
                if (queryNodes != null && queryNodesContainType(queryNodes, queryType)) {
                    return true;
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean queryNodesContainType(List<Object> nodes, String queryType) {
        for (Object node : nodes) {
            final Map<String, Object> nodeMap = (Map<String, Object>) node;
            if (queryType.equals(nodeMap.get("type"))) {
                return true;
            }
            final List<Object> children = (List<Object>) nodeMap.get("children");
            if (children != null && queryNodesContainType(children, queryType)) {
                return true;
            }
        }
        return false;
    }
}
