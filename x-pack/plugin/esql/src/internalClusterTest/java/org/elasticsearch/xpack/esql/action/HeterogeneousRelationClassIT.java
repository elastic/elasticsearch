/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code _class} and {@code _name} where they earn their existence: one result set whose rows came from relations of
 * different kinds. On a query naming a single relation the answer is whatever the user already typed; mixing an index
 * and a dataset is the case where a row cannot be attributed without asking it.
 *
 * <p>The two relations carry disjoint {@code id} ranges (index 0..4, dataset 100..104) so every row's provenance is
 * decidable from {@code id} alone, independently of what {@code _class} claims — the assertions therefore check the
 * column against a fact the column did not produce. Both are named under a common prefix so the same query can be
 * spelled as an explicit list or as one wildcard, which reach the same union by different routes.
 */
public class HeterogeneousRelationClassIT extends AbstractExternalDataSourceIT {

    private static final String INDEX = "het_class_idx";
    private static final int ROWS = 5;
    private static final int DATASET_BASE = 100;

    /** A second pair under their own prefix, so one pattern reaches two indices without widening het_class_*. */
    private static final String SPAN_A = "span_name_a";
    private static final String SPAN_B = "span_name_b";
    private static final int SPAN_A_BASE = 200;
    private static final int SPAN_B_BASE = 300;

    private String dataset;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Before
    public void loadBothSources() throws Exception {
        createIndexWithRows(INDEX, 0);
        createIndexWithRows(SPAN_A, SPAN_A_BASE);
        createIndexWithRows(SPAN_B, SPAN_B_BASE);

        StringBuilder csv = new StringBuilder("id:integer,label:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            csv.append(DATASET_BASE + i).append(",row").append(i).append('\n');
        }
        Path csvFile = createTempDir().resolve("het_class.csv");
        Files.writeString(csvFile, csv.toString(), StandardCharsets.UTF_8);
        LinkedHashMap<String, DatasetFieldMapping> columns = new LinkedHashMap<>();
        columns.put("id", new DatasetFieldMapping("integer", null));
        columns.put("label", new DatasetFieldMapping("keyword", null));
        dataset = registerStrictDataset("het_class_ds", StoragePath.fileUri(csvFile), columns, Map.of("format", "csv"));
    }

    /** Every row says which kind of relation produced it, and names it. */
    public void testExplicitIndexAndDatasetEachAnswerForThemselves() {
        assertBothKindsAnswer("FROM " + INDEX + ", " + dataset);
    }

    /** One wildcard reaching both relations is the same union, and every row still answers for its own leaf. */
    public void testWildcardMatchingBothAnswersPerLeaf() {
        assertBothKindsAnswer("FROM het_class_*");
    }

    /** Grouping on _class separates the branches, which is what a user asks the column for. */
    public void testGroupingOnClassSeparatesTheBranches() {
        List<List<Object>> rows = query(
            "FROM het_class_* METADATA _class | STATS rows = COUNT(*) BY _class | KEEP _class, rows | SORT _class ASC"
        );
        assertThat(rows, equalTo(List.of(List.of("dataset", (long) ROWS), List.of("index", (long) ROWS))));
    }

    /**
     * One EsRelation covers an index pattern, so _name cannot be a per-relation literal the way a dataset's is: these
     * rows all come from a single relation and must still name the concrete index each one came from. This is the claim
     * behind aliasing _index instead of folding, and it is the case a single-index query cannot distinguish.
     */
    public void testNameVariesWithinOneIndexPattern() {
        List<List<Object>> rows = query("FROM span_name_* METADATA _class, _name | KEEP id, _class, _name | SORT id ASC");
        assertThat("both indices contributed every row", rows.size(), equalTo(ROWS * 2));
        for (List<Object> row : rows) {
            int id = (Integer) row.get(0);
            assertThat("_class for id " + id, row.get(1), equalTo("index"));
            assertThat("_name for id " + id, row.get(2), equalTo(id >= SPAN_B_BASE ? SPAN_B : SPAN_A));
        }
    }

    /** _name and _index agree row by row: on an index _name is that attribute under another name, not a copy of it. */
    public void testNameAgreesWithIndexRowByRow() {
        List<List<Object>> rows = query("FROM span_name_* METADATA _index, _name | KEEP _index, _name");
        assertThat("both indices contributed every row", rows.size(), equalTo(ROWS * 2));
        for (List<Object> row : rows) {
            assertThat("_name should equal _index", row.get(1), equalTo(row.get(0)));
        }
    }

    private void createIndexWithRows(String index, int base) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "label", "type=keyword")
        );
        for (int i = 0; i < ROWS; i++) {
            client().prepareIndex(index).setSource("id", base + i, "label", "row" + i).get();
        }
        client().admin().indices().prepareRefresh(index).get();
    }

    /**
     * Runs the query both ways round and checks each row's _class and _name against its id, which says where the row
     * really came from.
     */
    private void assertBothKindsAnswer(String from) {
        List<List<Object>> rows = query(from + " METADATA _class, _name | KEEP id, _class, _name | SORT id ASC");
        assertThat("both relations contributed every row", rows.size(), equalTo(ROWS * 2));
        for (List<Object> row : rows) {
            int id = (Integer) row.get(0);
            boolean fromDataset = id >= DATASET_BASE;
            assertThat("_class for id " + id, row.get(1), equalTo(fromDataset ? "dataset" : "index"));
            assertThat("_name for id " + id, row.get(2), equalTo(fromDataset ? dataset : INDEX));
        }
    }

    private List<List<Object>> query(String esql) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(esql), TIMEOUT)) {
            return getValuesList(response);
        }
    }
}
