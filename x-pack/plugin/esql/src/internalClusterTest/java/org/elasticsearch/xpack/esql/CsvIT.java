/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramMapperPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CsvIT extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(CsvIT.class);

    private static InternalTestCluster cluster;

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvSpecReader.CsvTestCase testCase;
    private final String instructions;

    public CsvIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.instructions = instructions;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertThat("Not enough specs found " + urls, urls, hasSize(greaterThan(0)));
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @BeforeClass
    public static void setupCluster() throws Exception {
        logger.info("Creating test cluster");
        cluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            false,
            true,
            1,
            1,
            "esql_test_cluster",
            NodeConfigurationSource.EMPTY,
            0,
            "node_",
            List.of(
                getTestTransportPlugin(),
                EsqlPluginWithEnterpriseOrTrialLicense.class,
                AggregateMetricMapperPlugin.class,
                AnalyticsPlugin.class,
                ConstantKeywordMapperPlugin.class,
                // EnrichPlugin.class,
                ExponentialHistogramMapperPlugin.class,
                LocalStateInferencePlugin.class,
                MapperExtrasPlugin.class,
                SpatialPlugin.class,
                UnsignedLongMapperPlugin.class,
                VersionFieldPlugin.class,
                Wildcard.class
            ),
            Function.identity(),
            TEST_ENTITLEMENTS::addEntitledNodePaths
        );
        cluster.beforeTest(random());

        // setup data
        long start = System.currentTimeMillis();

        // TODO populate data lazily
        for (var dataset : CsvTestsDataLoader.CSV_DATASET_MAP.values()) {
            if (dataset.requiresInferenceEndpoint()) {
                continue;// TODO inference endpoint
            }
            logger.info("Creating dataset [{}]", dataset.indexName());
            assertAcked(
                cluster.client()
                    .admin()
                    .indices()
                    .prepareCreate(dataset.indexName())
                    .setMapping(CsvTestsDataLoader.readMappingFile(dataset))
                    .setSettings(dataset.loadSettings())
            );
            if (dataset.dataFileName() != null) {
                var bulk = cluster.client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (var document : CsvTestsDataLoader.readCsvDocuments(dataset.streamData(), dataset.allowSubFields())) {
                    bulk.add(
                        cluster.client()
                            .prepareIndex(dataset.indexName())
                            .setId(document.id())
                            .setSource(document.json().toString(), XContentType.JSON)
                    );
                }
                if (bulk.numberOfActions() > 0) {
                    var result = bulk.get();
                    assertFalse(
                        "Must load dataset [" + dataset.indexName() + "] successfully: " + result.buildFailureMessage(),
                        result.hasFailures()
                    );
                }
            }
        }

        // TODO enrich?
        // for (var policy : CsvTestsDataLoader.ENRICH_POLICIES) {
        // logger.info("Creating policy [{}]", policy.policyFileName());
        // var p = EnrichPolicy.fromXContent(
        // JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, policy.streamPolicy())
        // );
        // assertAcked(
        // cluster.client()
        // .execute(
        // PutEnrichPolicyAction.INSTANCE,
        // new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy.policyName(), p)
        // )
        // );
        // var response = cluster.client()
        // .execute(
        // ExecuteEnrichPolicyAction.INSTANCE,
        // new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy.policyName())
        // )
        // .actionGet();
        // assertTrue(response.getStatus().isCompleted());
        // }

        for (var view : CsvTestsDataLoader.VIEW_CONFIGS) {
            logger.info("Creating view [{}]", view.name());
            assertAcked(
                cluster.client()
                    .execute(
                        PutViewAction.INSTANCE,
                        new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(view.name(), view.loadQuery()))
                    )
            );
        }

        long stop = System.currentTimeMillis();
        logger.info("Created datasets in {} ms", stop - start);
    }

    @AfterClass
    public static void cleanupCluster() throws IOException {
        cluster.close();
    }

    public final void test() throws Throwable {
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));

        skipUnsupportedCapability(EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_V6);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_OPTIONS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_MULTIPLE_GROUPINGS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.RERANK);
        skipUnsupportedCapability(EsqlCapabilities.Cap.COMPLETION);
        // runs in a single cluster/single node mode
        skipUnsupportedCapability(EsqlCapabilities.Cap.METADATA_FIELDS_REMOTE_TEST);

        assumeFalse("Enrich is not supported in IT yet", testCase.query.trim().toUpperCase(java.util.Locale.ROOT).contains("ENRICH"));
        assumeFalse(
            "CSV tests cannot handle EXTERNAL sources (requires QA integration tests)",
            testCase.query.trim().toUpperCase(java.util.Locale.ROOT).startsWith("EXTERNAL")
        );

        var request = syncEsqlQueryRequest(testCase.query);
        try (var response = cluster.client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)) {
            ExpectedResults expected = loadCsvSpecValues(testCase.expectedResults);
            ActualResults actual = new ActualResults(
                response.zoneId(),
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                response.columns().stream().map(column -> CsvTestUtils.Type.asType(column.type().nameUpper())).toList(),
                response.columns().stream().map(ColumnInfoImpl::type).toList(),
                response.pages(),
                Map.of() // TODO get warning headers
            );
            CsvAssert.assertResults(expected, actual, testCase.ignoreOrder, logResults() ? logger : null);
        } catch (Throwable t) {
            t.setStackTrace(prependSpec(t.getStackTrace()));
            throw t;
        }
    }

    private StackTraceElement[] prependSpec(StackTraceElement[] original) {
        StackTraceElement[] copy = new StackTraceElement[original.length + 1];
        copy[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);
        System.arraycopy(original, 0, copy, 1, original.length);
        return copy;
    }

    private void skipUnsupportedCapability(EsqlCapabilities.Cap capability) {
        assumeFalse("Skipping unsupported capability: " + capability, testCase.requiredCapabilities.contains(capability.capabilityName()));
    }

    public boolean logResults() {
        return false;
    }
}
