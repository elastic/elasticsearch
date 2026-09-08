/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Shared S3/Parquet external csv-spec support for both mixed-cluster coordinator versions.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public abstract class AbstractMixedClusterExternalSpecIT extends AbstractExternalSourceSpecTestCase {

    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();
    private static final ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH, true);

    @ClassRule
    public static TestRule ruleChain = chainOuterRuleBeforeFixturesAndCluster(
        (base, description) -> new org.junit.runners.model.Statement() {
            @Override
            public void evaluate() throws Throwable {
                assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP object-store fixtures", inFipsJvm());
                assumeTrue(
                    "external data-source BWC coverage starts at 9.5.0",
                    MixedClusterTestSupport.bwcVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)
                );
                base.evaluate();
            }
        },
        cluster
    );

    protected AbstractMixedClusterExternalSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, StorageBackend.S3, "parquet");
    }

    /**
     * Selects which version receives the REST request and therefore coordinates the query.
     */
    protected abstract boolean oldCoordinator();

    @Override
    protected final String getTestRestCluster() {
        HttpHost[] allHosts = parseClusterHosts(cluster.getHttpAddresses()).toArray(HttpHost[]::new);
        try (RestClient probe = buildClient(restAdminSettings(), allHosts)) {
            ObjectPath nodes = ObjectPath.createFromResponse(probe.performRequest(new Request("GET", "/_nodes")));
            return MixedClusterTestSupport.httpAddressesForCoordinator(nodes, oldCoordinator());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve coordinator addresses from /_nodes", e);
        }
    }

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        if (requiresInferenceEndpointOnLocalCluster()) {
            CsvTestUtils.assumeTrueLogging(
                "Inference test service cannot be installed on BWC nodes",
                supportsInferenceTestServiceOnLocalCluster()
            );
        }
        CsvTestUtils.assumeTrueLogging(
            "Old mixed-cluster node does not support required capabilities for " + testName,
            testCase.requiredCapabilities.isEmpty() || hasCapabilities(adminClient(), testCase.requiredCapabilities)
        );
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster external tests don't support local cluster capability requirements",
            testCase.missingCapabilitiesLocalCluster.isEmpty()
        );
        CsvTestUtils.assumeTrueLogging(
            "Mixed-cluster external tests don't support remote cluster capability requirements",
            testCase.missingCapabilitiesRemoteCluster.isEmpty()
        );
        assumeTrue(
            "Test " + testName + " is skipped on " + MixedClusterTestSupport.bwcVersion(),
            isEnabled(testName, instructions, MixedClusterTestSupport.bwcVersion())
        );
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return false;
    }

    @Override
    protected void createInferenceEndpointsIfSupported() {
        // The current-only inference test plugin cannot be installed on the BWC nodes. Tests requiring its
        // reranker are skipped by the strict mixed-cluster capability checks below.
    }

    @Override
    protected boolean deduplicateExactWarnings() {
        return true;
    }
}
