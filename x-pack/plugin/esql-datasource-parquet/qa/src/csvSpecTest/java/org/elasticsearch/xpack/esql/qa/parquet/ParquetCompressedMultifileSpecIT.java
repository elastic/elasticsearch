/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy.BwcTestId;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceCodecEligibility;

import java.util.List;

/**
 * Parameterized integration tests for multifile Parquet with internal compression.
 * Runs multifile csv-spec tests against GZIP and ZSTD internal codecs only (these are the
 * codecs for which compressed multifile_split fixtures are generated at build time).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetCompressedMultifileSpecIT extends AbstractParquetExternalSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = BwcMatrixPolicy.compressed(
        StorageBackend.S3,
        "gzip",
        new BwcTestId("external-multifile.csv-spec", "readAllEmployeesMultiFile")
    );
    private static final List<String> CODECS = EsqlDataSourceCodecEligibility.parquetCodecs("gzip", "zstd");

    private final String codecName;

    public ParquetCompressedMultifileSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String codecName,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend);
        this.codecName = codecName;
    }

    @Override
    protected String multifileSplitDir() {
        return "multifile_split-" + codecName;
    }

    @Override
    protected String guardCodecIdentity() {
        return EsqlDataSourceCodecEligibility.normalizeCodecToken(codecName);
    }

    @Override
    protected BwcMatrixPolicy bwcMatrixPolicy() {
        return BWC_MATRIX_POLICY;
    }

    // Migrated specs run via FROM <dataset> on S3 and via the rebuilt EXTERNAL query on the other backends.
    // The reader: "java" this IT injects is redundant with the .parquet extension default (the codec lives
    // inside the .parquet file, so the extension is unchanged), so FROM-on-S3 still uses the Java reader.

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithCodecs(
            BWC_MATRIX_POLICY,
            CODECS,
            "/datasources/external-multifile.csv-spec",
            "/datasources/external-multifile-resolution.csv-spec"
        );
    }
}
