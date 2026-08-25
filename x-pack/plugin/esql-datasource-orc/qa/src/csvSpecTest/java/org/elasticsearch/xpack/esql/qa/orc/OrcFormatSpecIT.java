/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for standalone ORC files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class OrcFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = chainFixturesBeforeCluster(cluster);

    public OrcFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "orc");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static final Set<String> SKIPPED_TESTS = Set.of(
        // The mixed-temporal pair declares one file `date` and the other `date_nanos`, and ORC cannot
        // round-trip the nanos half: OrcFormatReader maps TIMESTAMP and TIMESTAMP_INSTANT to DATETIME
        // and has no DATE_NANOS mapping. OrcFixtureGenerator therefore refuses the type rather than
        // writing it as a string, and the fixture matrix declares that cell absent -- so these cases
        // have no data to read here. Reader work, not fixture work; re-enable when ORC gains
        // DATE_NANOS.
        "temporalWidensToMinMax",
        "temporalWidensToFilteredMinMax"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " needs DATE_NANOS support in the ORC reader", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec");
    }
}
