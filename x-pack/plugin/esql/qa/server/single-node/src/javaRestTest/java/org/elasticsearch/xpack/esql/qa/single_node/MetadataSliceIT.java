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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * Integration tests for the {@code _slice} metadata field in ES|QL.
 * Verifies that {@code METADATA _slice} can be requested, projected, and filtered
 * (with Lucene pushdown) against indices with {@code index.slice.enabled: true}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MetadataSliceIT extends ESRestTestCase {

    /**
     * Dedicated cluster with the {@code slice_indexing} feature flag enabled.
     * Without the flag {@code index.slice.enabled} is rejected at index creation and
     * the {@code _slice} metadata attribute is not registered in ES|QL.
     */
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(c -> c.feature(FeatureFlag.SLICE_INDEXING));

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    // Cached per JVM run — expensive to re-check for every test method.
    private static Boolean diskBBQAvailable = null;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Skips every test in this class when the {@code diskbbq} module is absent.
     * <p>
     * Slice-enabled indices require {@code DiskBBQPlugin.IndexSettingProvider} to inject
     * {@code index.slice.validated=true} at index-creation time. Without that injection the
     * {@link org.elasticsearch.index.IndexSettings} constructor throws an
     * {@link IllegalArgumentException} during shard initialisation, the shard never becomes
     * active, and every subsequent query fails with {@code no_shard_available_action_exception}.
     * <p>
     * This guard is needed because some CI distributions (e.g. the serverless project's stateful
     * check distribution) do not bundle {@code diskbbq}.
     */
    @Before
    public void assumeDiskBBQAvailable() throws IOException {
        if (diskBBQAvailable == null) {
            diskBBQAvailable = isModuleAvailable("diskbbq");
        }
        assumeTrue(
            "diskbbq module is not available in this distribution; "
                + "slice-enabled index shards cannot start without it (index.slice.validated injection missing)",
            diskBBQAvailable
        );
    }

    @SuppressWarnings("unchecked")
    private static boolean isModuleAvailable(String moduleName) throws IOException {
        for (Map<?, ?> nodeInfo : getNodesInfo(client()).values()) {
            List<Map<?, ?>> modules = (List<Map<?, ?>>) nodeInfo.get("modules");
            if (modules != null) {
                for (Map<?, ?> module : modules) {
                    if (moduleName.equals(module.get("name"))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Verifies that {@code FROM idx METADATA _slice | KEEP _slice} returns the routing
     * value for each document as the {@code _slice} column.
     */
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

    /**
     * Verifies that {@code WHERE _slice == "s1"} is pushed down to Lucene and returns
     * only documents whose routing matches the given slice value.
     */
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

    /**
     * Verifies that {@code WHERE _slice != "s1"} returns only documents with a different
     * slice value.
     */
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

    /**
     * Verifies that {@code LIKE} on {@code _slice} is pushed to Lucene via a doc-values
     * wildcard query and returns all documents whose routing matches the pattern.
     */
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

    /**
     * Verifies that {@code RLIKE} on {@code _slice} is pushed to Lucene via a doc-values
     * regexp query and returns all documents whose routing matches the pattern.
     */
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

    /**
     * Verifies that {@code OR} of slice equalities returns the union of both slices.
     */
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

    /**
     * Verifies that a conflicting AND ({@code _slice == "s1" AND _slice == "s2"}) returns
     * zero results — no document can satisfy both routing constraints simultaneously.
     */
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

    /**
     * Verifies that RENAME followed by WHERE on the renamed column works correctly.
     * This exercises the plan traversal path where the filter references an alias rather
     * than the original {@code _slice} attribute name.
     */
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

    /**
     * Verifies that {@code _slice} works correctly when the filter <em>cannot</em> be pushed
     * to Lucene and must be evaluated in the compute engine via the block loader.
     * {@code SUBSTRING(_slice, 1, 1)} is not a pushable predicate, so the optimizer keeps
     * the filter above the Lucene reader; ES|QL must load {@code _slice} values from doc
     * values and apply the condition in-memory.
     */
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

    /**
     * Verifies that {@code _slice} can be used in a STATS aggregation, which reads values
     * via the block loader without any filter pushdown.
     */
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

    // ---- helpers ----

    /**
     * Creates a slice-enabled index with {@code index.slice.enabled: true}.
     * {@code DiskBBQPlugin.IndexSettingProvider} injects {@code index.slice.validated=true}
     * automatically at creation time; without it the shard cannot start.
     */
    private void createSliceIndex(String index) throws IOException {
        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0
              }
            }
            """);
        assertOK(client().performRequest(createIndex));
    }

    /**
     * Indexes a document using the given slice value.
     * Slice-enabled indices require the {@code _slice} parameter rather than {@code routing}.
     */
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
