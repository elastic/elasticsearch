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
import static org.hamcrest.Matchers.equalTo;

/**
 * Type inference over a CSV or TSV column holding digits from outside ASCII. ClickHouse and DuckDB both type such a
 * column as a string and return the text; a CSV number is ASCII digits.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvNonAsciiDigitInferenceIT extends AbstractExternalDataSourceIT {

    private static final String ARABIC_INDIC_123 = "١٢٣";
    private static final String FULLWIDTH_123 = "１２３";
    private static final String DEVANAGARI_123 = "१२३";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private String register(String name, String ext, String body) throws Exception {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("d." + ext), body, StandardCharsets.UTF_8);
        return registerLocalFileDataset(name, StoragePath.fileUri(dir.resolve("d." + ext)), Map.of());
    }

    public void testNonAsciiDigitColumnStaysText() throws Exception {
        String ds = register("scripts", "csv", "code,qty\n" + ARABIC_INDIC_123 + ",1\n" + FULLWIDTH_123 + ",2\n" + DEVANAGARI_123 + ",3\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT qty | KEEP code"))) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(ARABIC_INDIC_123), List.of(FULLWIDTH_123), List.of(DEVANAGARI_123)))
            );
        }
    }

    /** One non-ASCII value in a column of ASCII whole numbers must not be folded into them. */
    public void testMixedColumnKeepsEveryValueAsWritten() throws Exception {
        String ds = register("mixed", "csv", "v,qty\n10,1\n20,2\n" + ARABIC_INDIC_123 + ",3\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT qty | KEEP v"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("10"), List.of("20"), List.of(ARABIC_INDIC_123))));
        }
    }

    public void testTsvNonAsciiDigitColumnStaysText() throws Exception {
        String ds = register("tsvscripts", "tsv", "code\tqty\n" + ARABIC_INDIC_123 + "\t1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP code"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(ARABIC_INDIC_123))));
        }
    }

    /** Control: ASCII whole numbers still infer as integers. */
    public void testAsciiDigitsStillInferInteger() throws Exception {
        String ds = register("ascii", "csv", "v,qty\n10,1\n20,2\n123,3\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | STATS s = SUM(v)"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(153L));
        }
    }
}
