/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Runs the same queries on a slice-enabled index through the search API and through ES|QL, and checks that both read the
 * same documents. The search API selects slices in the path of the request, ES|QL with a filter on {@code _slice}.
 * <p>
 * Every document carries a {@code key} made of its slice and its position, since the same id exists in every slice.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SliceQueryDslAndEsqlIT extends ESRestTestCase {

    private static final String INDEX = "tenants";
    private static final int DIMS = 16;
    private static final Map<String, Integer> DOCS_PER_SLICE = Map.of("acme", 9, "globex", 6, "initech", 4);
    private static final int ALL_DOCS = 19;

    /** The slices a request selects, as the search parameter and as the ES|QL condition. An empty list selects none. */
    private static final List<List<String>> SELECTIONS = List.of(
        List.of(),
        List.of("acme"),
        List.of("globex"),
        List.of("acme", "initech"),
        List.of("acme", "globex", "initech"),
        List.of("unknown")
    );

    /** A query in both languages: the body of a search query and the equivalent ES|QL condition. */
    private record Condition(String dsl, String esql) {}

    private static final List<Condition> CONDITIONS = List.of(
        new Condition("{ \"match_all\": {} }", "true"),
        new Condition("{ \"term\": { \"parity\": \"even\" } }", "parity == \"even\""),
        new Condition("{ \"terms\": { \"name\": [\"doc-0\", \"doc-3\", \"doc-7\"] } }", "name IN (\"doc-0\", \"doc-3\", \"doc-7\")"),
        new Condition("{ \"range\": { \"position\": { \"gte\": 2, \"lt\": 6 } } }", "position >= 2 AND position < 6"),
        new Condition("{ \"match\": { \"description\": \"alpha\" } }", "MATCH(description, \"alpha\")"),
        new Condition("{ \"prefix\": { \"name\": \"doc-1\" } }", "STARTS_WITH(name, \"doc-1\")"),
        new Condition(
            "{ \"bool\": { \"should\": [ { \"range\": { \"position\": { \"lt\": 2 } } }, { \"term\": { \"parity\": \"odd\" } } ] } }",
            "position < 2 OR parity == \"odd\""
        ),
        new Condition(
            "{ \"bool\": { \"filter\": [ { \"term\": { \"parity\": \"even\" } } ], \"must_not\": [ { \"term\": { \"position\": 0 } } ] } }",
            "parity == \"even\" AND position != 0"
        )
    );

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(c -> c.feature(FeatureFlag.SLICE_INDEXING));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("requires slice selection", EsqlCapabilities.Cap.SLICE_SELECTION_FROM_FILTER.isEnabled());
        if (indexExists(INDEX)) {
            return;
        }
        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(org.elasticsearch.core.Strings.format("""
            {
              "settings": { "index.slice.enabled": true, "index.number_of_shards": 3 },
              "mappings": {
                "properties": {
                  "key": { "type": "keyword" },
                  "name": { "type": "keyword" },
                  "parity": { "type": "keyword" },
                  "position": { "type": "integer" },
                  "description": { "type": "text" },
                  "vector": {
                    "type": "dense_vector",
                    "dims": %d,
                    "index": true,
                    "similarity": "l2_norm",
                    "index_options": { "type": "%s" }
                  }
                }
              }
            }
            """, DIMS, randomFrom("bbq_disk", "hnsw", "int8_hnsw", "flat")));
        assertOK(client().performRequest(createIndex));

        StringBuilder bulk = new StringBuilder();
        DOCS_PER_SLICE.forEach((slice, docs) -> {
            for (int i = 0; i < docs; i++) {
                bulk.append(org.elasticsearch.core.Strings.format("{\"index\": {\"_id\": \"%d\", \"_slice\": \"%s\"}}\n", i, slice));
                bulk.append(
                    org.elasticsearch.core.Strings.format(
                        "{\"key\": \"%s-%d\", \"name\": \"doc-%d\", \"parity\": \"%s\", \"position\": %d, "
                            + "\"description\": \"%s document of %s\", \"vector\": %s}\n",
                        slice,
                        i,
                        i,
                        i % 2 == 0 ? "even" : "odd",
                        i,
                        i % 3 == 0 ? "alpha" : "beta",
                        slice,
                        vector(slice, i)
                    )
                );
            }
        });
        Request request = new Request("POST", "/" + INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertThat(entityAsMap(client().performRequest(request)).get("errors"), equalTo(false));
    }

    /**
     * Both engines return the same documents for every query and every selection of slices, and only documents of the
     * selected slices.
     */
    public void testQueriesReadTheSameDocuments() throws IOException {
        int matched = 0;
        for (List<String> slices : SELECTIONS) {
            for (Condition condition : CONDITIONS) {
                String description = "slices " + slices + ", query " + condition.esql();
                TreeSet<String> fromSearch = new TreeSet<>(searchKeys(slices, "\"query\": " + condition.dsl() + ", \"size\": 100"));
                TreeSet<String> fromEsql = new TreeSet<>(
                    esqlKeys("FROM tenants METADATA _slice | WHERE " + esqlCondition(slices, condition.esql()) + " | KEEP key")
                );
                assertThat(description, fromSearch, equalTo(fromEsql));
                for (String key : fromSearch) {
                    assertTrue(description + " returned " + key, slices.isEmpty() || slices.contains(key.substring(0, key.indexOf('-'))));
                }
                matched += fromSearch.size();
            }
        }
        assertThat("the queries must match documents for the comparison to mean anything", matched, greaterThan(ALL_DOCS));
    }

    public void testCounts() throws IOException {
        for (List<String> slices : SELECTIONS) {
            int expected = slices.isEmpty() ? ALL_DOCS : slices.stream().mapToInt(slice -> DOCS_PER_SLICE.getOrDefault(slice, 0)).sum();

            Request count = new Request("POST", path(slices, "_count"));
            assertThat(slices.toString(), entityAsMap(client().performRequest(count)).get("count"), equalTo(expected));

            List<List<Object>> rows = esqlRows(
                "FROM tenants METADATA _slice | WHERE " + esqlCondition(slices, "true") + " | STATS c = COUNT(*)"
            );
            assertThat(slices.toString(), ((Number) rows.get(0).get(0)).intValue(), equalTo(expected));
        }
    }

    /**
     * The first documents by position, with the key breaking ties, come back in the same order.
     */
    public void testSortedTopDocuments() throws IOException {
        for (List<String> slices : SELECTIONS) {
            List<String> fromSearch = searchKeys(
                slices,
                "\"sort\": [ { \"position\": \"desc\" }, { \"key\": \"asc\" } ], \"size\": 5, "
                    + "\"query\": { \"term\": { \"parity\": \"even\" } }"
            );
            List<String> fromEsql = esqlKeys(
                "FROM tenants METADATA _slice | WHERE "
                    + esqlCondition(slices, "parity == \"even\"")
                    + " | SORT position DESC, key ASC | LIMIT 5 | KEEP key"
            );
            assertThat(slices.toString(), fromSearch, equalTo(fromEsql));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGroupedCounts() throws IOException {
        for (List<String> slices : SELECTIONS) {
            Request search = new Request("POST", path(slices, "_search"));
            search.setJsonEntity("{ \"size\": 0, \"aggs\": { \"by_parity\": { \"terms\": { \"field\": \"parity\" } } } }");
            Map<String, Object> aggregations = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("aggregations");
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) aggregations.get("by_parity")).get(
                "buckets"
            );
            Map<String, Integer> fromSearch = buckets.stream()
                .collect(Collectors.toMap(bucket -> (String) bucket.get("key"), bucket -> ((Number) bucket.get("doc_count")).intValue()));

            Map<String, Integer> fromEsql = new TreeMap<>();
            for (List<Object> row : esqlRows(
                "FROM tenants METADATA _slice | WHERE " + esqlCondition(slices, "true") + " | STATS c = COUNT(*) BY parity"
            )) {
                fromEsql.put((String) row.get(1), ((Number) row.get(0)).intValue());
            }
            assertThat(slices.toString(), new TreeMap<>(fromSearch), equalTo(fromEsql));
        }
    }

    /**
     * A nearest neighbour search needs the slices to search by name in both engines, and searches the neighbours within
     * them. The vectors closest to the query all live in slice {@code acme}, so a search that selected its neighbours
     * across slices would return nothing from the others.
     */
    public void testKnn() throws IOException {
        String queryVector = vector("acme", 0);
        String knnSearch = "\"knn\": { \"field\": \"vector\", \"query_vector\": " + queryVector + ", \"k\": 2, \"num_candidates\": 10 }";
        String knnFunction = "KNN(vector, " + queryVector + ", {\"k\": 2})";
        String rejected = "to perform knn search on field [vector], the slices to search must be selected";

        ResponseException fromSearch = expectThrows(ResponseException.class, () -> searchKeys(List.of(), knnSearch));
        assertThat(fromSearch.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(fromSearch.getMessage(), containsString(rejected));
        ResponseException fromEsql = expectThrows(
            ResponseException.class,
            () -> esqlKeys("FROM tenants | WHERE " + knnFunction + " | KEEP key | LIMIT 10")
        );
        assertThat(fromEsql.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(fromEsql.getMessage(), containsString(rejected));

        // selecting every slice does not name the slices to search
        Request allSlices = new Request("POST", path(List.of("_all"), "_search"));
        allSlices.setJsonEntity("{" + knnSearch + "}");
        ResponseException fromAll = expectThrows(ResponseException.class, () -> client().performRequest(allSlices));
        assertThat(fromAll.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(fromAll.getMessage(), containsString(rejected));

        for (List<String> slices : List.of(List.of("globex"), List.of("globex", "initech"))) {
            List<String> searchKeys = searchKeys(slices, knnSearch);
            List<String> esqlKeys = esqlKeys(
                "FROM tenants METADATA _slice | WHERE " + esqlCondition(slices, knnFunction) + " | KEEP key | LIMIT 10"
            );
            for (List<String> keys : List.of(searchKeys, esqlKeys)) {
                assertThat(slices.toString(), keys, not(empty()));
                assertThat(slices.toString(), keys, everyItem(not(startsWith("acme-"))));
            }
            // ES|QL returns the nearest neighbours of each segment, which include the k nearest overall that the search returns
            assertTrue(slices + ": " + searchKeys + " not in " + esqlKeys, esqlKeys.containsAll(searchKeys));
        }
    }

    private static String esqlCondition(List<String> slices, String condition) {
        if (slices.isEmpty()) {
            return condition;
        }
        String names = slices.stream().map(slice -> "\"" + slice + "\"").collect(Collectors.joining(", "));
        return "_slice IN (" + names + ") AND (" + condition + ")";
    }

    /** The path of an endpoint of the index, restricted to the given slices when there are any. */
    private static String path(List<String> slices, String endpoint) {
        return slices.isEmpty() ? "/" + INDEX + "/" + endpoint : "/" + INDEX + "/" + String.join(",", slices) + "/" + endpoint;
    }

    /** The keys of the hits of a search on the given slices, in the order of the hits. */
    @SuppressWarnings("unchecked")
    private static List<String> searchKeys(List<String> slices, String body) throws IOException {
        Request search = new Request("POST", path(slices, "_search"));
        search.setJsonEntity("{ \"_source\": false, \"fields\": [\"key\"], " + body + " }");
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("hits");
        List<String> keys = new ArrayList<>();
        for (Map<String, Object> hit : (List<Map<String, Object>>) hits.get("hits")) {
            keys.add(((List<String>) ((Map<String, Object>) hit.get("fields")).get("key")).get(0));
        }
        return keys;
    }

    /** The first column of the rows of an ES|QL query, in the order of the rows. */
    private static List<String> esqlKeys(String query) throws IOException {
        List<String> keys = new ArrayList<>();
        for (List<Object> row : esqlRows(query)) {
            keys.add((String) row.get(0));
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> esqlRows(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("allow_partial_results", "false");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query).endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        // queries without a LIMIT warn about the default one
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return (List<List<Object>>) entityAsMap(client().performRequest(request)).get("values");
    }

    /**
     * The vectors of slice {@code acme} sit next to each other, far from those of the other slices.
     */
    private static String vector(String slice, int position) {
        float first = "acme".equals(slice) ? 1f : -1f;
        StringBuilder vector = new StringBuilder("[").append(first).append(", ").append(position * 0.001f);
        for (int i = 2; i < DIMS; i++) {
            vector.append(", 0.0");
        }
        return vector.append("]").toString();
    }
}
