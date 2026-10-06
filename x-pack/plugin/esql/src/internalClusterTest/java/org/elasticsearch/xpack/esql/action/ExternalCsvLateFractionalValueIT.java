/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A decimal in a CSV column whose inferred type is a whole number. Each defect case accepts a refused read that
 * names the value, or the value as the file holds it, and fails on a rounded value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvLateFractionalValueIT extends AbstractExternalDataSourceIT {

    private static final Object REFUSED = new Object();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private static Path write(Path dir, String fileName, String body) throws Exception {
        Files.createDirectories(dir);
        Path file = dir.resolve(fileName);
        Files.writeString(file, body, StandardCharsets.UTF_8);
        return file;
    }

    /** A header {@code id,v}, then {@code wholeRows} rows whose {@code v} is {@code base + id}, then one row holding {@code last}. */
    private static String wholeRowsThen(int wholeRows, long base, String last) {
        StringBuilder sb = new StringBuilder("id,v\n");
        for (int i = 1; i <= wholeRows; i++) {
            sb.append(i).append(',').append(base + i).append('\n');
        }
        return sb.append(wholeRows + 1).append(',').append(last).append('\n').toString();
    }

    /** The single value the query returns, or {@link #REFUSED} when the read failed with a message naming {@code written}. */
    private Object valueOrRefusal(String query, String written) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            return rows.get(0).get(0);
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("[" + written + "]"));
            return REFUSED;
        }
    }

    private static void assertNotRounded(Object value, Object written) {
        if (value != REFUSED) {
            assertThat(value, equalTo(written));
        }
    }

    /** No settings: the sample and the widening window cover 40,000 rows, and the decimal is row 40,001. */
    public void testDecimalPastDefaultInferenceWindowIsNotRounded() throws Exception {
        Path file = write(createTempDir().resolve("late_default"), "d.csv", wholeRowsThen(40_000, 0, "1.9"));
        String dataset = registerLocalFileDataset("late_default", StoragePath.fileUri(file), Map.of());
        assertNotRounded(valueOrRefusal("FROM " + dataset + " | WHERE id == 40001 | KEEP v", "1.9"), 1.9);
    }

    /** A glob whose first file holds whole numbers types the column from that file alone. */
    public void testDecimalInLaterGlobFileIsNotRounded() throws Exception {
        Path dir = createTempDir().resolve("glob_decimal");
        write(dir, "a.csv", "id,v\n1,10\n2,20\n");
        write(dir, "b.csv", "id,v\n3,1.9\n4,2.5\n");
        String dataset = registerLocalFileDataset(
            "glob_decimal",
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("file_sort_by", "name", "file_order", "asc")
        );
        assertNotRounded(valueOrRefusal("FROM " + dataset + " | WHERE id == 3 | KEEP v", "1.9"), 1.9);
    }

    /** The same for a column inferred {@code long}. */
    public void testDecimalInInferredLongColumnIsNotRounded() throws Exception {
        Path file = write(createTempDir().resolve("late_long"), "d.csv", wholeRowsThen(20, 3_000_000_000L, "1.9"));
        String dataset = registerLocalFileDataset("late_long", StoragePath.fileUri(file), Map.of("schema_sample_size", 5));
        assertNotRounded(valueOrRefusal("FROM " + dataset + " | WHERE id == 21 | KEEP v", "1.9"), 1.9);
    }

    // Controls: correct today, and a fix must keep them.

    /** A decimal inside the widening window widens the column. */
    public void testDecimalInsideWideningWindowWidensColumn() throws Exception {
        Path file = write(createTempDir().resolve("widen_window"), "d.csv", wholeRowsThen(8, 0, "1.9"));
        String dataset = registerLocalFileDataset("widen_window", StoragePath.fileUri(file), Map.of("schema_sample_size", 5));
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 9 | KEEP v", "1.9"), equalTo(1.9));
    }

    /** A whole number past the inference window reads exactly. */
    public void testWholeNumberPastInferenceWindowIsExact() throws Exception {
        Path file = write(createTempDir().resolve("late_whole"), "d.csv", wholeRowsThen(20, 0, "2010"));
        String dataset = registerLocalFileDataset("late_whole", StoragePath.fileUri(file), Map.of("schema_sample_size", 5));
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 21 | KEEP v", "2010"), equalTo(2010));
    }

    /** A column declared {@code integer} in the mapping refuses a non-whole decimal. */
    public void testDeclaredIntegerColumnRefusesDecimal() throws Exception {
        Path file = write(createTempDir().resolve("declared"), "d.csv", "id,v\n1,10\n2,1.9\n");
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("v", new DatasetFieldMapping("integer", null));
        String dataset = registerNonStrictDataset("declared_integer", StoragePath.fileUri(file), properties, Map.of());
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 2 | KEEP v", "1.9"), sameInstance(REFUSED));
    }

    /** A column typed {@code integer} in the file's own header refuses a non-whole decimal. */
    public void testTypedHeaderIntegerColumnRefusesDecimal() throws Exception {
        Path file = write(createTempDir().resolve("typed_header"), "d.csv", "id:integer,v:integer\n1,10\n2,1.9\n");
        String dataset = registerLocalFileDataset("typed_header", StoragePath.fileUri(file), Map.of());
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 2 | KEEP v", "1.9"), sameInstance(REFUSED));
    }
}
