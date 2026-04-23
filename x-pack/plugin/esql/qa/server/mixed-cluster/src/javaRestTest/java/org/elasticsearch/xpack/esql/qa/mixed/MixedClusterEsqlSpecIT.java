/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

public class MixedClusterEsqlSpecIT extends EsqlSpecTestCase {
    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH);

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(
        System.getProperty("tests.old_cluster_version") != null
            ? System.getProperty("tests.old_cluster_version").replace("-SNAPSHOT", "")
            : null
    );
    private static RestClient oldNodeClient = null;

    public MixedClusterEsqlSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        IOUtils.close(oldNodeClient);
        oldNodeClient = null;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        CsvTestUtils.assumeTrueLogging(
            "Old mixed-cluster node does not support required capabilities for " + testName,
            testCase.requiredCapabilities.isEmpty() || hasCapabilities(oldNodeClient(), testCase.requiredCapabilities)
        );
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, instructions, bwcVersion));
    }

    private RestClient oldNodeClient() throws IOException {
        if (oldNodeClient == null) {
            oldNodeClient = buildClient(restAdminSettings(), new HttpHost[] { HttpHost.create("http://" + cluster.getHttpAddress(0)) });
        }
        return oldNodeClient;
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected boolean supportsIndexModeLookup() {
        return hasCapabilities(adminClient(), List.of(JOIN_LOOKUP_V12.capabilityName()));
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected boolean deduplicateExactWarnings() {
        /*
         * In ESQL's main tests we shouldn't have to deduplicate but in
         * serverless, where we reuse this test case exactly with *slightly*
         * different configuration, we must deduplicate. So we do it here.
         * It's a bit of a loss of precision, but that's ok.
         */
        return true;
    }
}
