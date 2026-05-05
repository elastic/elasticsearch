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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Runs the same Parquet format spec tests as ParquetFormatSpecIT but using the parquet-rs native reader.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetRsFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public ParquetRsFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
    }

    @Override
    protected String readerName() {
        return FormatNameResolver.READER_PARQUET_RS;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    private static final Set<String> SKIPPED_TESTS = Set.of(
        // unknown parquet column [job_positions] referenced in projection (reported in the schema as "element")
        "filterFirstRowAllColumns",
        "mvAppendFromScalars",
        "mvConcatFromSplit",
        "mvCountFromSplit",
        "mvDedupeFromSplit",
        "mvExpandFromSplit",
        "mvMaxFromSplit",
        "mvMinFromSplit",
        // unknown parquet column [salary_change] referenced in projection (reported in the schema as "element")
        "mvDedupeFromSplit2",
        // unknown parquet column [author] referenced in projection
        "externalRerankBooks"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by parquet-rs reader", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec");
    }
}
