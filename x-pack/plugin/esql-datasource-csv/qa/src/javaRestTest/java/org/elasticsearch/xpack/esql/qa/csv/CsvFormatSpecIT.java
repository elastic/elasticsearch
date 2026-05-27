/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.util.List;

/**
 * Parameterized integration tests for standalone CSV files.
 * Each csv-spec test is run against every configured storage backend.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class CsvFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public CsvFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "csv");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    // CSV reads only the csv-*.csv-spec files. The shared external-*.csv-spec files read the
    // multi-value employees fixture, which under the default multi_value_syntax: none does not parse
    // as CSV (the commas inside [a,b] misalign columns); scalar coverage lives in csv-basic.csv-spec
    // (bracket-free employees twin) and multi-value coverage in csv-multivalue.csv-spec.
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/csv-*.csv-spec");
    }
}
