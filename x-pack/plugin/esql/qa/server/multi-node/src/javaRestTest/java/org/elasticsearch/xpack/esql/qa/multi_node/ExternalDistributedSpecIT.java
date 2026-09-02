/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Runs external source csv-spec tests on a 3-node cluster with each distribution
 * strategy (coordinator_only, round_robin, adaptive). The key invariant is that all
 * three modes produce identical results for every query; divergence flags a split
 * assignment, exchange, or aggregation bug.
 */
@ThreadLeakFilters(
    filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class, ExternalDistributedSpecIT.AzureSdkThreadFilter.class }
)
public class ExternalDistributedSpecIT extends AbstractExternalSourceSpecTestCase {

    public static class AzureSdkThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("azure-sdk-");
        }
    }

    private static final List<String> DISTRIBUTION_MODES = List.of("coordinator_only", "round_robin", "adaptive");

    private static final ElasticsearchCluster CLUSTER_INSTANCE = ExternalDistributedClusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = chainOuterRuleBeforeFixturesAndCluster(
        (base, description) -> new org.junit.runners.model.Statement() {
            @Override
            public void evaluate() throws Throwable {
                assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
                base.evaluate();
            }
        },
        CLUSTER_INSTANCE
    );

    private final String distributionMode;

    public ExternalDistributedSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend,
        String distributionMode
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
        this.distributionMode = distributionMode;
    }

    @Override
    protected String getTestRestCluster() {
        return CLUSTER_INSTANCE.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> backendTests = readExternalSpecTests("/external-basic.csv-spec", "/external-multivalue.csv-spec");
        List<Object[]> parameterizedTests = new ArrayList<>();
        for (Object[] backendTest : backendTests) {
            for (String mode : DISTRIBUTION_MODES) {
                Object[] extended = new Object[backendTest.length + 1];
                System.arraycopy(backendTest, 0, extended, 0, backendTest.length);
                extended[backendTest.length] = mode;
                parameterizedTests.add(extended);
            }
        }
        return parameterizedTests;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);
        // Every spec in this suite now runs via FROM <dataset>; gate on that capability rather than
        // probing for the EXTERNAL command's storage connectors.
        assumeTrue(
            "FROM <dataset> requires the [dataset_in_from_command] capability",
            hasCapabilities(client(), List.of(EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName()))
        );
    }

    @Override
    protected void addRandomPragma(Settings.Builder pragma) {
        super.addRandomPragma(pragma);
        pragma.put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode);
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }
}
