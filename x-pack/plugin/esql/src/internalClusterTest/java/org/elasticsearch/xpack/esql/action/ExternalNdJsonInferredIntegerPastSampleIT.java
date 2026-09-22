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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
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
import static org.hamcrest.Matchers.equalTo;

/**
 * An NDJSON column's inferred type is a description of the values schema inference saw. A value the inference did
 * not see and that contradicts the description must not be rewritten into the inferred type with the rounding a
 * declared type licenses. Each defect case accepts either outcome a correct reader may give, a refused read or the
 * value as the file holds it, and rejects only the rewritten value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalNdJsonInferredIntegerPastSampleIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    private Path write(String dirName, String fileName, String content) throws Exception {
        Path dir = createTempDir().resolve(dirName);
        Files.createDirectories(dir);
        Path file = dir.resolve(fileName);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    /** Runs the query and returns its single value, or null when the read refused the data. */
    private Object singleValueOrRefused(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            return rows.get(0).get(0);
        } catch (Exception e) {
            logger.info("read refused the value", e);
            return null;
        }
    }

    private static String ndjsonWholeNumbersThen(int wholeRows, String lastValue) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= wholeRows; i++) {
            sb.append("{\"id\":").append(i).append(",\"v\":").append(i * 10).append("}\n");
        }
        sb.append("{\"id\":").append(wholeRows + 1).append(",\"v\":").append(lastValue).append("}\n");
        return sb.toString();
    }

    public void testNdjsonFractionalPastDefaultSampleIsNotRounded() throws Exception {
        Path file = write("ndjson_default_sample", "late.ndjson", ndjsonWholeNumbersThen(20_000, "1.9"));
        String dataset = registerDataset("ndjson_late_fraction", StoragePath.fileUri(file), Map.of());
        Object v = singleValueOrRefused("FROM " + dataset + " | WHERE id == 20001 | KEEP v");
        if (v != null) {
            assertThat("the decimal the file holds", v, equalTo(1.9));
        }
    }

    public void testNdjsonFractionalStringPastSampleIsNotRounded() throws Exception {
        Path file = write("ndjson_string_fraction", "late.ndjson", ndjsonWholeNumbersThen(2, "\"2.5\""));
        String dataset = registerDataset("ndjson_late_string_fraction", StoragePath.fileUri(file), Map.of("schema_sample_size", 2));
        Object v = singleValueOrRefused("FROM " + dataset + " | WHERE id == 3 | KEEP v");
        if (v != null) {
            assertThat("the text the file holds", v, equalTo("2.5"));
        }
    }

    public void testNdjsonFractionalInLaterGlobFileIsNotRounded() throws Exception {
        Path dir = createTempDir().resolve("ndjson_glob_fraction");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.ndjson"), "{\"id\":1,\"v\":10}\n{\"id\":2,\"v\":20}\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.ndjson"), "{\"id\":3,\"v\":1.9}\n{\"id\":4,\"v\":2.5}\n", StandardCharsets.UTF_8);
        String dataset = registerLocalFileDataset(
            "ndjson_glob_fraction",
            StoragePath.fileUri(dir) + "/*.ndjson",
            Map.of("file_sort_by", "name", "file_order", "asc")
        );
        Object v = singleValueOrRefused("FROM " + dataset + " | WHERE id == 3 | KEEP v");
        if (v != null) {
            assertThat("the decimal the second file holds", v, equalTo(1.9));
        }
    }

    // Controls: behaviour that is correct today and that a fix must keep.

    public void testNdjsonFractionalInsideSampleWidensColumn() throws Exception {
        Path file = write("ndjson_in_sample", "in.ndjson", ndjsonWholeNumbersThen(2, "1.9"));
        String dataset = registerDataset("ndjson_in_sample_fraction", StoragePath.fileUri(file), Map.of("schema_sample_size", 3));
        assertThat(singleValueOrRefused("FROM " + dataset + " | WHERE id == 3 | KEEP v"), equalTo(1.9));
    }

    public void testNdjsonDeclaredIntegerStillRounds() throws Exception {
        Path file = write("ndjson_declared", "declared.ndjson", ndjsonWholeNumbersThen(2, "1.9"));
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("v", new DatasetFieldMapping("integer", null));
        String dataset = registerStrictDataset("ndjson_declared_integer", StoragePath.fileUri(file), properties, Map.of());
        assertThat(singleValueOrRefused("FROM " + dataset + " | WHERE id == 3 | KEEP v"), equalTo(2));
    }

}
