/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * A multi-file CSV glob read under {@code schema_resolution = union_by_name} where one file's shared column is
 * inferred numeric from its sampled prefix but actually carries a text value further down the file, while another
 * file infers the same column as text.
 * <p>
 * Reconciliation widens the column to {@code KEYWORD} across the files
 * ({@code SchemaReconciliation#reconcileUnionByName}), so the whole column is a string in the unified schema. The
 * text value in the numeric-inferred file must therefore survive the read as its raw text: the whole point of
 * widening to {@code KEYWORD} is that every token is a valid string.
 * <p>
 * {@code schema_sample_size} is set small so the numeric-inferred file's sample sees only its leading numeric rows
 * and never the trailing text row; that reproduces the sampling mismatch (numeric-looking prefix, text tail) with a
 * tiny fixture instead of a 20k-row file. {@code error_mode = null_field} keeps the read from aborting so any lost
 * value shows up as a {@code null} cell rather than a query failure.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvUnionByNameNumericWidenedToKeywordIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testNumericInferredColumnKeepsTextTailWhenWidenedToKeyword() throws Exception {
        Path dir = createTempDir().resolve("ubn_numeric_widened_keyword");
        Files.createDirectories(dir);
        // a.csv: with schema_sample_size=2 the sampler sees only rows (1,100) and (2,200) -> col inferred numeric,
        // but row (3,oops) carries a text value the sample never saw.
        Files.writeString(dir.resolve("a.csv"), "id,col,note\n1,100,alpha\n2,200,beta\n3,oops,gamma\n", StandardCharsets.UTF_8);
        // b.csv: col is text in every sampled row -> inferred keyword, so union_by_name widens col to keyword.
        Files.writeString(dir.resolve("b.csv"), "id,col,note\n4,abc,delta\n5,def,epsilon\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset(
            "ubn_numeric_widened_keyword",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );

        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            // col is keyword in the unified schema, so numeric tokens read back as their string form. The row-3 text
            // value "oops" is the one the sample never saw; it must survive rather than null-fill.
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains("100", "200", "oops", "abc", "def"));
            assertThat("the numeric-inferred file's text tail must not be dropped", rows.get(2).get(0), equalTo("oops"));
        }
    }
}
