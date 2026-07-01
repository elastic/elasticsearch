/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * Integration tests for the {@code _slice} metadata field in ES|QL.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MetadataSliceIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(c -> c.feature(FeatureFlag.SLICE_INDEXING));

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testMetadataSliceReturnsRoutingValues() throws IOException {
        String index = "slice_test_read";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s1");
        indexDocWithSlice(index, "3", "s2");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<Map<?, ?>> columns = (List<Map<?, ?>>) result.get("columns");
        assertEquals(1, columns.size());
        assertEquals("_slice", columns.get(0).get("name"));
        assertEquals("keyword", columns.get(0).get("type"));

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals(3, values.size());
        List<String> sliceValues = values.stream().map(row -> (String) row.get(0)).collect(Collectors.toList());
        assertEquals(List.of("s1", "s1", "s2"), sliceValues);
    }

    public void testWhereSliceEqualityFilterPushdown() throws IOException {
        String index = "slice_test_filter";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");
        indexDocWithSlice(index, "3", "s1");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice == \"s1\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("WHERE _slice == 's1' should return exactly 2 documents", 2, values.size());
        for (List<?> row : values) {
            assertEquals("s1", row.get(0));
        }
    }

    public void testWhereSliceNotEqualityFilterPushdown() throws IOException {
        String index = "slice_test_neq";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");
        indexDocWithSlice(index, "3", "s3");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice != \"s1\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("WHERE _slice != 's1' should return documents s2 and s3", 2, values.size());
        assertEquals("s2", values.get(0).get(0));
        assertEquals("s3", values.get(1).get(0));
    }

    public void testWhereSliceLikeFilterPushdown() throws IOException {
        String index = "slice_test_like";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1a");
        indexDocWithSlice(index, "2", "s1b");
        indexDocWithSlice(index, "3", "s2");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice LIKE \"s1*\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("LIKE 's1*' should match s1a and s1b", 2, values.size());
        assertEquals("s1a", values.get(0).get(0));
        assertEquals("s1b", values.get(1).get(0));
    }

    public void testWhereSliceRlikeFilterPushdown() throws IOException {
        String index = "slice_test_rlike";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");
        indexDocWithSlice(index, "3", "s3");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice RLIKE \"s[12]\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("RLIKE 's[12]' should match s1 and s2 but not s3", 2, values.size());
        assertEquals("s1", values.get(0).get(0));
        assertEquals("s2", values.get(1).get(0));
    }

    public void testWhereSliceOrFilter() throws IOException {
        String index = "slice_test_or";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");
        indexDocWithSlice(index, "3", "s3");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice == \"s1\" OR _slice == \"s2\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("OR should return s1 and s2 but not s3", 2, values.size());
        assertEquals("s1", values.get(0).get(0));
        assertEquals("s2", values.get(1).get(0));
    }

    public void testWhereSliceConflictingAndReturnsNoResults() throws IOException {
        String index = "slice_test_and_conflict";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE _slice == \"s1\" AND _slice == \"s2\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertTrue("conflicting AND on _slice must return zero results", values == null || values.isEmpty());
    }

    public void testRenameSliceThenFilter() throws IOException {
        String index = "slice_test_rename";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s2");
        indexDocWithSlice(index, "3", "s1");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query(
                "FROM " + index + " METADATA _slice | RENAME _slice AS my_slice | WHERE my_slice == \"s1\" | KEEP my_slice | SORT my_slice"
            );
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("RENAME then filter should return 2 docs with s1", 2, values.size());
        for (List<?> row : values) {
            assertEquals("s1", row.get(0));
        }
    }

    public void testNonPushdownFilterViaBlockLoader() throws IOException {
        String index = "slice_test_non_pushdown";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "apple");
        indexDocWithSlice(index, "2", "apricot");
        indexDocWithSlice(index, "3", "banana");

        refreshIndex(index);

        // SUBSTRING is not a Lucene pushdown predicate, so the filter stays in the compute engine
        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | WHERE SUBSTRING(_slice, 1, 1) == \"a\" | KEEP _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("SUBSTRING filter should match 'apple' and 'apricot' but not 'banana'", 2, values.size());
        assertEquals("apple", values.get(0).get(0));
        assertEquals("apricot", values.get(1).get(0));
    }

    public void testStatsGroupBySlice() throws IOException {
        String index = "slice_test_stats";
        createSliceIndex(index);

        indexDocWithSlice(index, "1", "s1");
        indexDocWithSlice(index, "2", "s1");
        indexDocWithSlice(index, "3", "s2");

        refreshIndex(index);

        RestEsqlTestCase.RequestObjectBuilder builder = RestEsqlTestCase.requestObjectBuilder()
            .query("FROM " + index + " METADATA _slice | STATS c = COUNT(*) BY _slice | SORT _slice");
        Map<String, Object> result = runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);

        @SuppressWarnings("unchecked")
        List<List<?>> values = (List<List<?>>) result.get("values");
        assertEquals("should have two groups: s1 and s2", 2, values.size());
        assertEquals("s1", values.get(0).get(1));
        assertEquals(2, ((Number) values.get(0).get(0)).intValue());
        assertEquals("s2", values.get(1).get(1));
        assertEquals(1, ((Number) values.get(1).get(0)).intValue());
    }

    private void createSliceIndex(String index) throws IOException {
        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "index.number_of_shards": 1
              }
            }
            """);
        assertOK(client().performRequest(createIndex));
        ensureYellowAndNoInitializingShards(index, null);
    }

    private void indexDocWithSlice(String index, String id, String slice) throws IOException {
        Request req = new Request("PUT", "/" + index + "/_doc/" + id);
        req.addParameter("_slice", slice);
        req.setJsonEntity("{\"value\":\"" + id + "\"}");
        assertOK(client().performRequest(req));
    }

    private void refreshIndex(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }
}
