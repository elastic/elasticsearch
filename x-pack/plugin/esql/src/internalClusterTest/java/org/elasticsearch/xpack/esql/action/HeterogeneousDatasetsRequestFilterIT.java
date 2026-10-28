/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/**
 * The experience of applying an out-of-band request {@code filter} to <em>several datasets with different schemas queried
 * at once</em> ({@code FROM dsA, dsB}). The filter is inserted independently above each dataset leaf and bound against that
 * leaf's own schema, so a field present on one dataset and absent on another is filtered on the first and treated as null
 * on the second — exactly how co-queried indices with heterogeneous mappings behave.
 *
 * <p>Two datasets carry disjoint {@code id} ranges (dsA 0..19, dsB 100..119) so every returned row's provenance is
 * decidable from {@code id} alone. dsA has a {@code region} column; dsB does not. Each case asserts the exact set of
 * {@code id}s returned — proving that a dataset lacking the filtered field drops out under a positive filter and returns
 * in full under a negation, and that the applied clauses on the other dataset still bite.
 */
public class HeterogeneousDatasetsRequestFilterIT extends AbstractExternalDataSourceIT {

    private static final int N = 20;
    private static final int BASE_A = 0;
    private static final int BASE_B = 100;

    private String dsA; // id, region, status
    private String dsB; // id, status (no region)

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private static String region(int i) {
        return i % 2 == 0 ? "eu" : "us";
    }

    private static int status(int i) {
        return 200 + (i % 3) * 100; // 200, 300, 400
    }

    @Before
    public void registerDatasets() throws Exception {
        StringBuilder a = new StringBuilder("id:integer,region:keyword,status:integer\n");
        for (int i = 0; i < N; i++) {
            a.append(BASE_A + i).append(',').append(region(i)).append(',').append(status(i)).append('\n');
        }
        Path fileA = createTempDir().resolve("dsA.csv");
        Files.writeString(fileA, a.toString(), StandardCharsets.UTF_8);
        LinkedHashMap<String, DatasetFieldMapping> colsA = new LinkedHashMap<>();
        colsA.put("id", new DatasetFieldMapping("integer", null));
        colsA.put("region", new DatasetFieldMapping("keyword", null));
        colsA.put("status", new DatasetFieldMapping("integer", null));
        dsA = registerStrictDataset("het_ds_a", StoragePath.fileUri(fileA), colsA, Map.of("format", "csv"));

        StringBuilder b = new StringBuilder("id:integer,status:integer\n");
        for (int i = 0; i < N; i++) {
            b.append(BASE_B + i).append(',').append(status(i)).append('\n');
        }
        Path fileB = createTempDir().resolve("dsB.csv");
        Files.writeString(fileB, b.toString(), StandardCharsets.UTF_8);
        LinkedHashMap<String, DatasetFieldMapping> colsB = new LinkedHashMap<>();
        colsB.put("id", new DatasetFieldMapping("integer", null));
        colsB.put("status", new DatasetFieldMapping("integer", null));
        dsB = registerStrictDataset("het_ds_b", StoragePath.fileUri(fileB), colsB, Map.of("format", "csv"));
    }

    /** The id set returned by {@code FROM dsA, dsB} under a request filter, sorted ascending. */
    private List<Object> idsUnderFilter(QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dsA + ", " + dsB + " | KEEP id | SORT id ASC").filter(filter);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            return getValuesList(response).stream().map(row -> row.get(0)).toList();
        }
    }

    private static List<Object> idsA(IntPredicate keep) {
        List<Object> ids = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            if (keep.test(i)) {
                ids.add(BASE_A + i);
            }
        }
        return ids;
    }

    private static List<Object> allB() {
        List<Object> ids = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            ids.add(BASE_B + i);
        }
        return ids;
    }

    /**
     * The headline: a positive filter on a field only dsA has selects dsA's matches and drops dsB entirely (dsB's rows
     * treat {@code region} as null, and {@code null == "eu"} is false) — identical to a co-queried index without the field.
     */
    public void testPositiveFilterOnAFieldAbsentFromOneDatasetDropsThatDataset() {
        List<Object> ids = idsUnderFilter(QueryBuilders.termQuery("region", "eu"));
        assertEquals("dsA's eu rows only; dsB contributes nothing", idsA(i -> region(i).equals("eu")), ids);
    }

    /** The negation of the same: dsB, whose exclusion cannot bite an absent field, comes back in full. */
    public void testNegatedFilterOnAnAbsentFieldReadmitsThatDatasetInFull() {
        List<Object> ids = idsUnderFilter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("region", "eu")));
        List<Object> expected = new ArrayList<>(idsA(i -> region(i).equals("eu") == false));
        expected.addAll(allB()); // NOT(null == "eu") == NOT(false) == true for every dsB row
        assertEquals(expected, ids);
    }

    /** An AND of an absent-field clause with a present-field clause: dsB is already excluded by the absent clause. */
    public void testConjunctionAbsentFieldStillExcludesDespiteAnApplicableClause() {
        QueryBuilder filter = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("region", "eu"))
            .must(QueryBuilders.rangeQuery("status").gte(300));
        List<Object> ids = idsUnderFilter(filter);
        assertEquals(
            "dsA rows that are eu AND status>=300; dsB still none (absent region excludes it)",
            idsA(i -> region(i).equals("eu") && status(i) >= 300),
            ids
        );
    }

    /** A clause on a field BOTH datasets have applies to both. */
    public void testFilterOnAFieldPresentInAllDatasetsAppliesEverywhere() {
        List<Object> ids = idsUnderFilter(QueryBuilders.termQuery("status", 300));
        List<Object> expected = new ArrayList<>(idsA(i -> status(i) == 300));
        for (int i = 0; i < N; i++) {
            if (status(i) == 300) {
                expected.add(BASE_B + i);
            }
        }
        assertEquals(expected, ids);
    }

    /** A field no dataset has: a positive filter selects nothing (both treat it as null). */
    public void testPositiveFilterOnAFieldAbsentEverywhereReturnsNoRows() {
        assertThat(idsUnderFilter(QueryBuilders.termQuery("nope", "x")), empty());
    }

    /** ...and its negation returns everything from both. */
    public void testNegatedFilterOnAFieldAbsentEverywhereReturnsAllRows() {
        List<Object> ids = idsUnderFilter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("nope", "x")));
        List<Object> expected = new ArrayList<>(idsA(i -> true));
        expected.addAll(allB());
        assertEquals(expected, ids);
    }

    /** Smoke: no filter shape at all (match_all) returns every row of both datasets. */
    public void testMatchAllReturnsEveryRowOfEveryDataset() {
        List<Object> ids = idsUnderFilter(QueryBuilders.matchAllQuery());
        List<Object> expected = new ArrayList<>(idsA(i -> true));
        expected.addAll(allB());
        assertEquals(expected, ids);
    }
}
