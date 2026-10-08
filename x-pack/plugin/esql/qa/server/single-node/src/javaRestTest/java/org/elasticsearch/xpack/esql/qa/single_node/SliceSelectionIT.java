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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThan;

/**
 * Integration tests for the slices a source reads, which a query selects by filtering the source on {@code _slice}.
 * <p>
 * Two indices hold the same three slices. Slice {@code near} holds the vectors closest to the query vector used by the
 * nearest neighbour tests, so a search that selects its neighbours across slices returns nothing from the other slices.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SliceSelectionIT extends ESRestTestCase {

    private static final String INDEX = "tenants";
    private static final String FROM = "FROM tenants METADATA _slice ";
    private static final String FROM_SINGLE = "FROM tenants-single METADATA _slice ";
    /** Every slice shares the only shard, so that a restriction to a slice cannot come from shard routing alone. */
    private static final String SINGLE_SHARD_INDEX = "tenants-single";
    private static final int SHARDS = 5;
    private static final int DIMS = 64;

    private static final int NEAR_DOCS = 20;
    private static final int FAR_DOCS = 4;
    private static final int OTHER_DOCS = 3;
    private static final int ALL_DOCS = NEAR_DOCS + FAR_DOCS + OTHER_DOCS;

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
        String vectorIndexType = randomFrom("bbq_disk", "hnsw", "int8_hnsw", "flat");
        createSliceIndex(INDEX, SHARDS, vectorIndexType);
        createSliceIndex(SINGLE_SHARD_INDEX, 1, vectorIndexType);
        StringBuilder bulk = new StringBuilder();
        for (String index : List.of(INDEX, SINGLE_SHARD_INDEX)) {
            for (int i = 0; i < NEAR_DOCS; i++) {
                addDoc(bulk, index, "near", i, vector(1f, i * 0.001f));
            }
            for (int i = 0; i < FAR_DOCS; i++) {
                addDoc(bulk, index, "far", i, vector(-1f, i * 0.001f));
            }
            for (int i = 0; i < OTHER_DOCS; i++) {
                addDoc(bulk, index, "other", i, vector(0f, 1f + i * 0.001f));
            }
        }
        Request request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("errors"), equalTo(false));
    }

    public void testFilterSelectsSlices() throws IOException {
        assertThat(count(FROM + "| WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(FAR_DOCS));
        assertThat(count(FROM + "| WHERE _slice IN (\"far\", \"other\") | STATS c = COUNT(*)"), equalTo(FAR_DOCS + OTHER_DOCS));
        assertThat(count(FROM + "| WHERE _slice == \"far\" OR _slice == \"near\" | STATS c = COUNT(*)"), equalTo(FAR_DOCS + NEAR_DOCS));
        assertThat(count(FROM + "| WHERE _slice == \"unknown\" | STATS c = COUNT(*)"), equalTo(0));
        assertThat(count(FROM + "| STATS c = COUNT(*)"), equalTo(ALL_DOCS));

        // slices that share a shard
        assertThat(count(FROM_SINGLE + "| WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(FAR_DOCS));
        assertThat(count(FROM_SINGLE + "| WHERE _slice IN (\"far\", \"near\") | STATS c = COUNT(*)"), equalTo(FAR_DOCS + NEAR_DOCS));
        List<String> sorted = slices(FROM_SINGLE + "| WHERE _slice == \"far\" | SORT position | KEEP _slice");
        assertThat(sorted, equalTo(List.of("far", "far", "far", "far")));
    }

    /**
     * A filter that selects slices only queries the shards that hold them. A filter that does not is still applied, on all
     * the shards.
     */
    public void testSelectedSlicesRouteTheQuery() throws IOException {
        assertThat(totalShards(FROM + "| STATS c = COUNT(*)"), equalTo(SHARDS));
        assertThat(totalShards(FROM + "| WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(1));
        assertThat(totalShards(FROM + "| WHERE position >= 0 | EVAL x = 1 | WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(1));
        assertThat(totalShards(FROM + "| WHERE _slice IN (\"far\", \"near\") | STATS c = COUNT(*)"), lessThan(SHARDS));

        for (String filter : List.of(
            "| WHERE _slice == \"far\" OR position > 100 ",
            "| WHERE _slice LIKE \"fa*\" ",
            "| WHERE _slice != \"near\" AND _slice != \"other\" ",
            "| SORT position, name | LIMIT 1000 | WHERE _slice == \"far\" "
        )) {
            String query = FROM + filter + "| STATS c = COUNT(*)";
            assertThat(query, totalShards(query), equalTo(SHARDS));
            assertThat(query, count(query), equalTo(FAR_DOCS));
        }
    }

    /**
     * A filter after a command that produces its own rows applies to those rows, not to the source.
     */
    public void testFilterAfterLimitOrStatsIsAnOrdinaryFilter() throws IOException {
        // the first documents by position come from every slice, of which only some belong to the slice
        List<String> rows = slices(FROM_SINGLE + "| SORT position, name | LIMIT 3 | WHERE _slice == \"far\" | KEEP _slice");
        assertThat(rows, equalTo(List.of("far")));

        // a filter on a grouping key stays above the STATS, so it does not sit on the source and selects no slice
        List<List<Object>> groups = values(FROM + "| STATS c = COUNT(*) BY _slice | WHERE _slice == \"far\"");
        assertThat(groups, equalTo(List.of(List.of(FAR_DOCS, "far"))));
        assertThat(totalShards(FROM + "| STATS c = COUNT(*) BY _slice | WHERE _slice == \"far\""), equalTo(SHARDS));
    }

    /**
     * The nearest neighbours are selected among the documents of the selected slices. The vectors closest to the query all
     * live in slice {@code near}, so a search that selects them across slices returns nothing from slice {@code far}.
     */
    public void testKnnSearchesTheSelectedSlices() throws IOException {
        String knn = "KNN(vector, " + queryVector() + ", {\"k\": 2})";

        // the search may return more than k documents, since k only bounds the neighbours each segment contributes
        for (String query : List.of(
            FROM_SINGLE + "| WHERE " + knn + " AND _slice == \"far\" | KEEP _slice, name | LIMIT 10",
            FROM_SINGLE + "| WHERE _slice == \"far\" | WHERE " + knn + " | KEEP _slice, name | LIMIT 10",
            FROM_SINGLE + "| WHERE " + knn + " | WHERE _slice == \"far\" | KEEP _slice, name | LIMIT 10"
        )) {
            List<String> far = slices(query);
            assertThat(query, far.size(), greaterThanOrEqualTo(2));
            assertThat(query, far, everyItem(equalTo("far")));
        }

        List<String> farAndOther = slices(
            FROM_SINGLE + "| WHERE " + knn + " AND _slice IN (\"far\", \"other\") | KEEP _slice, name | LIMIT 10"
        );
        assertThat(farAndOther.size(), greaterThanOrEqualTo(2));
        assertThat(farAndOther, everyItem(in(List.of("far", "other"))));

        List<List<Object>> rows = values(
            FROM_SINGLE + "| WHERE " + knn + " AND _slice == \"far\" AND position >= 2 | KEEP _slice, position | SORT position | LIMIT 10"
        );
        assertThat(rows, equalTo(List.of(List.of("far", 2), List.of("far", 3))));
    }

    /**
     * A knn function selects its documents on its own, so the vector field refuses to search a slice-enabled index when the
     * query does not say which slices to search. The query fails as a whole, whether or not partial results are allowed.
     */
    public void testKnnRequiresSelectedSlices() {
        String knn = "KNN(vector, " + queryVector() + ", {\"k\": 2})";
        for (String query : List.of(
            "FROM tenants | WHERE " + knn + " | LIMIT 10",
            FROM + "| WHERE " + knn + " AND _slice != \"near\" | LIMIT 10",
            FROM + "| WHERE " + knn + " AND _slice LIKE \"fa*\" | LIMIT 10",
            FROM + "| WHERE " + knn + " AND (_slice == \"far\" OR position > 2) | LIMIT 10",
            FROM + "| WHERE (" + knn + " AND _slice == \"far\") OR position > 100 | LIMIT 10",
            FROM + "| WHERE " + knn + " | SORT position | LIMIT 10 | WHERE _slice == \"far\""
        )) {
            for (boolean allowPartialResults : List.of(true, false)) {
                ResponseException e = expectThrows(ResponseException.class, query, () -> query(query, allowPartialResults));
                assertThat(query, e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                assertThat(
                    query,
                    e.getMessage(),
                    containsString("to perform knn search on field [vector], the slices to search must be selected")
                );
            }
        }
    }

    /**
     * A knn function and-ed with a condition that Lucene cannot evaluate does not run as a nearest neighbour search: it
     * scores every document instead, and the vector field needs no slice for that.
     */
    public void testExactKnnNeedsNoSlices() throws IOException {
        String knn = "KNN(vector, " + queryVector() + ", {\"k\": 2})";
        List<String> rows = slices(FROM + "| WHERE " + knn + " AND TO_LOWER(_slice) == \"far\" | KEEP _slice | LIMIT 100");
        assertThat(rows, hasSize(FAR_DOCS));
        assertThat(rows, everyItem(equalTo("far")));
    }

    /**
     * Other search functions run on a slice-enabled index like any other condition: on the slices the filter selects, or
     * on all of them.
     */
    public void testOtherSearchFunctions() throws IOException {
        String match = "MATCH(description, \"document\")";
        for (String commands : List.of(
            "| WHERE " + match + " AND _slice == \"far\" ",
            "| WHERE _slice == \"far\" | WHERE description : \"document\" ",
            "| WHERE _slice == \"far\" AND QSTR(\"description:document\") ",
            "| WHERE _slice == \"far\" AND KQL(\"description:document\") ",
            "| EVAL tenant = _slice | WHERE tenant == \"far\" AND " + match + " "
        )) {
            assertThat(commands, count(FROM + commands + "| STATS c = COUNT(*)"), equalTo(FAR_DOCS));
        }
        assertThat(count(FROM + "| WHERE _slice == \"far\" | STATS c = COUNT(*) WHERE " + match), equalTo(FAR_DOCS));
        assertThat(count("FROM tenants | WHERE " + match + " | STATS c = COUNT(*)"), equalTo(ALL_DOCS));
        assertThat(count(FROM + "| WHERE " + match + " AND _slice != \"near\" | STATS c = COUNT(*)"), equalTo(FAR_DOCS + OTHER_DOCS));
    }

    /**
     * {@code _slice} is null on an index without slices, so a filter on it matches none of its documents. Nothing fails,
     * and nothing fails.
     */
    public void testIndexWithoutSlices() throws IOException {
        createPlainIndex("plain", 3);

        assertThat(count("FROM plain METADATA _slice | WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(0));
        assertThat(count("FROM tenants, plain METADATA _slice | WHERE _slice == \"far\" | STATS c = COUNT(*)"), equalTo(FAR_DOCS));
        assertThat(count("FROM tenants, plain METADATA _slice | STATS c = COUNT(*)"), equalTo(ALL_DOCS + 3));
        assertThat(count("FROM tenants, plain | WHERE MATCH(name, \"plain-1\") | STATS c = COUNT(*)"), equalTo(1));
    }

    /**
     * Each subquery is a source of its own, with the slices its own filter selects.
     */
    public void testSubqueriesSelectTheirOwnSlices() throws IOException {
        assumeTrue("requires subqueries in FROM", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        createPlainIndex("plain", 3);

        assertThat(count("FROM (" + FROM + "| WHERE _slice == \"far\"), plain | STATS c = COUNT(*)"), equalTo(FAR_DOCS + 3));
        assertThat(
            count("FROM (" + FROM + "| WHERE _slice == \"far\"), (" + FROM_SINGLE + "| WHERE _slice == \"other\") | STATS c = COUNT(*)"),
            equalTo(FAR_DOCS + OTHER_DOCS)
        );
        // a filter over the subqueries applies to the rows of all of them
        assertThat(
            count("FROM (" + FROM + "), (" + FROM_SINGLE + "| WHERE position >= 1) | WHERE _slice == \"far\" | STATS c = COUNT(*)"),
            equalTo(FAR_DOCS + FAR_DOCS - 1)
        );
        assertThat(
            count("FROM (" + FROM + "| WHERE _slice == \"far\"), plain | WHERE _slice == \"other\" | STATS c = COUNT(*)"),
            equalTo(0)
        );
    }

    private static int count(String query) throws IOException {
        List<List<Object>> values = values(query);
        assertThat(values, hasSize(1));
        return ((Number) values.get(0).get(0)).intValue();
    }

    /** The values of the {@code _slice} column, which must be the first column of the result. */
    private static List<String> slices(String query) throws IOException {
        List<String> slices = new ArrayList<>();
        for (List<Object> row : values(query)) {
            slices.add((String) row.get(0));
        }
        return slices;
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> values(String query) throws IOException {
        return (List<List<Object>>) query(query).get("values");
    }

    @SuppressWarnings("unchecked")
    private static int totalShards(String query) throws IOException {
        Map<String, Object> clusters = (Map<String, Object>) query(query).get("_clusters");
        Map<String, Object> details = (Map<String, Object>) clusters.get("details");
        Map<String, Object> local = (Map<String, Object>) details.get("(local)");
        Map<String, Object> shards = (Map<String, Object>) local.get("_shards");
        assertThat(shards.get("failed"), equalTo(0));
        return ((Number) shards.get("total")).intValue();
    }

    private static Map<String, Object> query(String query) throws IOException {
        return query(query, false);
    }

    private static Map<String, Object> query(String query, boolean allowPartialResults) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("allow_partial_results", Boolean.toString(allowPartialResults));
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query).field("include_execution_metadata", true).endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        // queries without a LIMIT warn about the default one
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        return entityAsMap(client().performRequest(request));
    }

    private static void createSliceIndex(String index, int shards, String vectorIndexType) throws IOException {
        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity(org.elasticsearch.core.Strings.format("""
            {
              "settings": {
                "index.slice.enabled": true,
                "index.number_of_shards": %d
              },
              "mappings": {
                "properties": {
                  "name": { "type": "keyword" },
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
            """, shards, DIMS, vectorIndexType));
        assertOK(client().performRequest(createIndex));
    }

    private static void createPlainIndex(String index, int docs) throws IOException {
        if (indexExists(index)) {
            return;
        }
        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity("""
            { "mappings": { "properties": { "name": { "type": "keyword" }, "position": { "type": "integer" } } } }
            """);
        assertOK(client().performRequest(createIndex));
        for (int i = 0; i < docs; i++) {
            Request indexDoc = new Request("PUT", "/" + index + "/_doc/" + i);
            indexDoc.addParameter("refresh", "true");
            indexDoc.setJsonEntity("{\"name\": \"plain-" + i + "\", \"position\": " + i + "}");
            assertOK(client().performRequest(indexDoc));
        }
    }

    /**
     * Every slice uses the same document ids: a document is identified by its id and its slice.
     */
    private static void addDoc(StringBuilder bulk, String index, String slice, int position, String vector) {
        bulk.append(
            org.elasticsearch.core.Strings.format(
                "{\"index\": {\"_index\": \"%s\", \"_id\": \"%d\", \"slice\": \"%s\"}}\n",
                index,
                position,
                slice
            )
        );
        bulk.append(
            org.elasticsearch.core.Strings.format(
                "{\"name\": \"%s-%d\", \"position\": %d, \"description\": \"a document of %s\", \"vector\": %s}\n",
                slice,
                position,
                position,
                slice,
                vector
            )
        );
    }

    private static String queryVector() {
        return vector(1f, 0f);
    }

    private static String vector(float first, float second) {
        StringBuilder vector = new StringBuilder("[").append(first).append(", ").append(second);
        for (int i = 2; i < DIMS; i++) {
            vector.append(", 0.0");
        }
        return vector.append("]").toString();
    }
}
