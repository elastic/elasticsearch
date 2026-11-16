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
import static org.hamcrest.Matchers.equalTo;

/**
 * Type inference over a CSV column whose values are spellings the Java floating-point grammar accepts
 * but the CSV dialect does not define as numbers.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvJavaFloatSpellingIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private String register(String name, String body) throws Exception {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("d.csv"), body, StandardCharsets.UTF_8);
        return registerLocalFileDataset(name, StoragePath.fileUri(dir.resolve("d.csv")), Map.of());
    }

    /** A part-number / size column whose values end in a letter. The letters must survive. */
    public void testTypeSuffixedCodesStayStrings() throws Exception {
        String dataset = register("suffixed", "code,qty\n10D,1\n12d,2\n14F,3\n3f,4\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP code"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("10D"), List.of("12d"), List.of("14F"), List.of("3f"))));
        }
    }

    /** A hexadecimal-float spelling is not a CSV number. */
    public void testHexFloatSpellingStaysString() throws Exception {
        String dataset = register("hexfloat", "code,qty\n0x1p3,1\n0x1p4,2\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP code"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("0x1p3"), List.of("0x1p4"))));
        }
    }

    /** Control: a NaN/Infinity column keeps inferring as a double. */
    public void testNanAndInfinityWordsInferNumeric() throws Exception {
        String dataset = register("nanwords", "code,qty\nNaN,1\nInfinity,2\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP code"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(Double.NaN), List.of(Double.POSITIVE_INFINITY))));
        }
    }

    /** Control: a leading plus keeps inferring as a number. */
    public void testLeadingPlusInfersNumeric() throws Exception {
        String dataset = register("leadingplus", "code,qty\n+5,1\n+6,2\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS s = SUM(code)"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(11L));
        }
    }

    /** Control: a column of ordinary decimals must still infer as a number. */
    public void testOrdinaryDecimalsStillInferNumeric() throws Exception {
        String dataset = register("decimals", "v,qty\n10.5,1\n12.25,2\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS s = SUM(v)"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(22.75));
        }
    }

    /** An otherwise-whole-number column holding one suffixed code loses both its type and that code. */
    public void testWholeNumberColumnMeetingTypeSuffixKeepsItsValues() throws Exception {
        String dataset = register("intmix", "v,qty\n10,1\n20,2\n10D,3\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP v"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("10"), List.of("20"), List.of("10D"))));
        }
    }

    /** Control: a dynamic:true overlay declaring only this one column as keyword keeps the letters. */
    public void testDeclaredOverlayOnOneColumnKeepsTheLetters() throws Exception {
        Path dir = createTempDir().resolve("overlay");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("d.csv"), "code,qty\n10D,1\n12d,2\n", StandardCharsets.UTF_8);
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("code", new DatasetFieldMapping("keyword", null));
        String dataset = registerNonStrictDataset("overlay_one", StoragePath.fileUri(dir.resolve("d.csv")), props, Map.of());

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP code, qty"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("10D", 1), List.of("12d", 2))));
        }
    }

    /** Control: letters other than the four Java type suffixes already keep the column a string. */
    public void testOtherTrailingLettersAlreadyStayStrings() throws Exception {
        String dataset = register("othersuffix", "code,qty\n10A,1\n14B,2\n3C,3\n10E,4\n");

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT qty | KEEP code"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("10A"), List.of("14B"), List.of("3C"), List.of("10E"))));
        }
    }
}
