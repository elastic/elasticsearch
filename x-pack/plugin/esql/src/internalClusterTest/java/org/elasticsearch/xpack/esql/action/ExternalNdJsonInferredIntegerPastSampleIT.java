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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * An NDJSON column's inferred type is a description of the values schema inference saw. A later value that
 * contradicts that description must not be rewritten into the inferred type (rounded or truncated). Declared and
 * inferred whole-number columns alike refuse a non-whole decimal. Each defect case accepts a refused read whose
 * message names the value, or the value as the file holds it, and rejects only the rewritten value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalNdJsonInferredIntegerPastSampleIT extends AbstractExternalDataSourceIT {

    private static final Object REFUSED = new Object();

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
        assertNotRounded(valueOrRefusal("FROM " + dataset + " | WHERE id == 20001 | KEEP v", "1.9"), 1.9);
    }

    public void testNdjsonFractionalStringPastSampleIsNotRounded() throws Exception {
        Path file = write("ndjson_string_fraction", "late.ndjson", ndjsonWholeNumbersThen(2, "\"2.5\""));
        String dataset = registerDataset("ndjson_late_string_fraction", StoragePath.fileUri(file), Map.of("schema_sample_size", 2));
        // The column is an int block, so no correct reader can hand back the text; a refusal naming 2.5 is the only pass.
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 3 | KEEP v", "2.5"), sameInstance(REFUSED));
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
        assertNotRounded(valueOrRefusal("FROM " + dataset + " | WHERE id == 3 | KEEP v", "1.9"), 1.9);
    }

    // Controls: behaviour that is correct today and that a fix must keep.

    public void testNdjsonFractionalInsideSampleWidensColumn() throws Exception {
        Path file = write("ndjson_in_sample", "in.ndjson", ndjsonWholeNumbersThen(2, "1.9"));
        String dataset = registerDataset("ndjson_in_sample_fraction", StoragePath.fileUri(file), Map.of("schema_sample_size", 3));
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 3 | KEEP v", "1.9"), equalTo(1.9));
    }

    public void testNdjsonDeclaredIntegerRefusesDecimal() throws Exception {
        Path file = write("ndjson_declared", "declared.ndjson", ndjsonWholeNumbersThen(2, "1.9"));
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("v", new DatasetFieldMapping("integer", null));
        String dataset = registerStrictDataset("ndjson_declared_integer", StoragePath.fileUri(file), properties, Map.of());
        assertThat(valueOrRefusal("FROM " + dataset + " | WHERE id == 3 | KEEP v", "1.9"), sameInstance(REFUSED));
    }

}
