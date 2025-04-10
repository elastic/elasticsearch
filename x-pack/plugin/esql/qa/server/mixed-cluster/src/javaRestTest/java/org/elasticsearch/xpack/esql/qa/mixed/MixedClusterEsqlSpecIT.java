/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.JOIN_LOOKUP_V12;

public class MixedClusterEsqlSpecIT extends EsqlSpecTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    private static TestFeatureService oldClusterTestFeatureService = null;

    @Before
    public void extractOldClusterFeatures() {
        if (oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = testFeatureService;
        }
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null;
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    @AfterClass
    public static void cleanUp() {
        oldClusterTestFeatureService = null;
    }

    public MixedClusterEsqlSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        Mode mode
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, mode);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, instructions, bwcVersion));
    }

    @Override
    protected boolean supportTimeSeriesCommand() {
        return false;
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsInferenceTestService() {
        return false;
    }

    @Override
    protected boolean supportsIndexModeLookup() throws IOException {
        return hasCapabilities(List.of(JOIN_LOOKUP_V12.capabilityName()));
    }

    @Override
    protected boolean supportsSourceFieldMapping() throws IOException {
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
