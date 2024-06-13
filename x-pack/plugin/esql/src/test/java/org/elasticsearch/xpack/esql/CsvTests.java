/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.CsvSpecReader;
import org.elasticsearch.xpack.esql.core.SpecReader;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.index.IndexResolution;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.planner.TestPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadPageFromCsv;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.cap;
import static org.elasticsearch.xpack.esql.core.CsvSpecReader.specParser;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * CSV-based unit testing.
 *
 * Queries and their result live *.csv-spec files.
 * The results used in these files were manually added by running the same query on a real (debug mode) ES node. CsvTestsDataLoader loads
 * the test data helping to get the said results.
 *
 * CsvTestsDataLoader creates an index using the mapping in mapping-default.json. The same mapping file is also used to create the
 * IndexResolver that helps validate the correctness of the query and the supported field data types.
 * The created index and this class uses the data from employees.csv file as data. This class is creating one Page with Blocks in it using
 * this file and the type of blocks matches the type of the schema specified on the first line of the csv file. These being said, the
 * mapping in mapping-default.csv and employees.csv should be more or less in sync. An exception to this rule:
 *
 * languages:integer,languages.long:long. The mapping has "long" as a sub-field of "languages". ES knows what to do with sub-field, but
 * employees.csv is specifically defining "languages.long" as "long" and also has duplicated columns for these two.
 *
 * ATM the first line from employees.csv file is not synchronized with the mapping itself.
 *
 * When we add support for more field types, CsvTests should change to support the new Block types. Same goes for employees.csv file
 * (the schema needs adjustment) and the mapping-default.json file (to add or change an existing field).
 *
 * To log the results logResults() should return "true".
 */
// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class CsvTests extends ESTestCase {
    private static final Logger LOGGER = LogManager.getLogger(CsvTests.class);

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvSpecReader.CsvTestCase testCase;
    private final EsqlParser parser = new EsqlParser();
    private ThreadPool threadPool;
    private TestQueryRunner queryRunner;

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertThat("Not enough specs found " + urls, urls, hasSize(greaterThan(0)));
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Executor executor;
        if (randomBoolean()) {
            int numThreads = randomBoolean() ? 1 : between(2, 16);
            threadPool = new TestThreadPool(
                "CsvTests",
                new FixedExecutorBuilder(Settings.EMPTY, "esql_test", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
            );
            executor = threadPool.executor("esql_test");
        } else {
            threadPool = new TestThreadPool(getTestName());
            executor = threadPool.executor(ThreadPool.Names.SEARCH);
        }
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        queryRunner = new TestQueryRunner(threadPool, executor);
    }

    @After
    public void teardown() {
        HeaderWarning.removeThreadContext(threadPool.getThreadContext());
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public CsvTests(String fileName, String groupName, String testName, Integer lineNumber, CsvSpecReader.CsvTestCase testCase) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
    }

    public final void test() throws Throwable {
        try {
            assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, Version.CURRENT));

            if (Build.current().isSnapshot()) {
                assertThat(
                    "nonexistent capabilities declared as required",
                    testCase.requiredCapabilities,
                    everyItem(in(EsqlCapabilities.CAPABILITIES))
                );
            }

            /*
             * The csv tests support all but a few features. The unsupported features
             * are tested in integration tests.
             */
            assumeFalse("metadata fields aren't supported", testCase.requiredCapabilities.contains(cap(EsqlFeatures.METADATA_FIELDS)));
            assumeFalse("enrich can't load fields in csv tests", testCase.requiredCapabilities.contains(cap(EsqlFeatures.ENRICH_LOAD)));
            assumeFalse("can't load metrics in csv tests", testCase.requiredCapabilities.contains(cap(EsqlFeatures.METRICS_SYNTAX)));

            doTest();
        } catch (Throwable th) {
            throw reworkException(th);
        }
    }

    @Override
    protected final boolean enableWarningsCheck() {
        return false;  // We use our own warnings check
    }

    public boolean logResults() {
        return false;
    }

    private void doTest() throws Exception {
        var parsed = parser.createStatement(testCase.query);
        var testDataset = testsDataset(parsed);
        var indexResolution = loadIndexResolution(testDataset.mappingFileName(), testDataset.indexName());
        var enrichPolicies = loadEnrichPolicies();

        var actualResults = queryRunner.executePlan(parsed, testOperationProviders(testDataset), indexResolution, enrichPolicies);
        try {
            var expected = loadCsvSpecValues(testCase.expectedResults);

            var log = logResults() ? LOGGER : null;
            assertResults(expected, actualResults, testCase.ignoreOrder, log);
            assertWarnings(actualResults.responseHeaders().getOrDefault("Warning", List.of()));
        } finally {
            Releasables.close(() -> Iterators.map(actualResults.pages().iterator(), p -> p::releaseBlocks));
            assertThat(queryRunner.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        }
    }

    protected void assertResults(ExpectedResults expected, ActualResults actual, boolean ignoreOrder, Logger logger) {
        CsvAssert.assertResults(expected, actual, ignoreOrder, logger);
        /*
         * Comment the assertion above and enable the next two lines to see the results returned by ES without any assertions being done.
         * This is useful when creating a new test or trying to figure out what are the actual results.
         */
        // CsvTestUtils.logMetaData(actual.columnNames(), actual.columnTypes(), LOGGER);
        // CsvTestUtils.logData(actual.values(), LOGGER);
    }

    private static IndexResolution loadIndexResolution(String mappingName, String indexName) {
        var mapping = new TreeMap<>(loadMapping(mappingName));
        return IndexResolution.valid(new EsIndex(indexName, mapping, Set.of(indexName)));
    }

    private static EnrichResolution loadEnrichPolicies() {
        EnrichResolution enrichResolution = new EnrichResolution();
        for (CsvTestsDataLoader.EnrichConfig policyConfig : CsvTestsDataLoader.ENRICH_POLICIES) {
            EnrichPolicy policy = loadEnrichPolicyMapping(policyConfig.policyFileName());
            CsvTestsDataLoader.TestsDataset sourceIndex = CSV_DATASET_MAP.get(policy.getIndices().get(0));
            // this could practically work, but it's wrong:
            // EnrichPolicyResolution should contain the policy (system) index, not the source index
            EsIndex esIndex = loadIndexResolution(sourceIndex.mappingFileName(), sourceIndex.indexName()).get();
            var concreteIndices = Map.of(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, Iterables.get(esIndex.concreteIndices(), 0));
            enrichResolution.addResolvedPolicy(
                policyConfig.policyName(),
                Enrich.Mode.ANY,
                new ResolvedEnrichPolicy(
                    policy.getMatchField(),
                    policy.getType(),
                    policy.getEnrichFields(),
                    concreteIndices,
                    esIndex.mapping()
                )
            );
        }
        return enrichResolution;
    }

    private static EnrichPolicy loadEnrichPolicyMapping(String policyFileName) {
        URL policyMapping = CsvTestsDataLoader.class.getResource("/" + policyFileName);
        assertThat(policyMapping, is(notNullValue()));
        try {
            String fileContent = CsvTestsDataLoader.readTextFile(policyMapping);
            return EnrichPolicy.fromXContent(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, fileContent));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read resource " + policyFileName);
        }
    }

    private static CsvTestsDataLoader.TestsDataset testsDataset(LogicalPlan parsed) {
        var preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        var indices = preAnalysis.indices;
        if (indices.size() == 0) {
            /*
             * If the data set doesn't matter we'll just grab one we know works.
             * Employees is fine.
             */
            return CSV_DATASET_MAP.get("employees");
        } else if (preAnalysis.indices.size() > 1) {
            throw new IllegalArgumentException("unexpected index resolution to multiple entries [" + preAnalysis.indices.size() + "]");
        }

        String indexName = indices.get(0).id().index();
        var dataset = CSV_DATASET_MAP.get(indexName);
        if (dataset == null) {
            throw new IllegalArgumentException("unknown CSV dataset for table [" + indexName + "]");
        }
        return dataset;
    }

    private static TestPhysicalOperationProviders testOperationProviders(CsvTestsDataLoader.TestsDataset dataset) throws Exception {
        var testData = loadPageFromCsv(CsvTests.class.getResource("/" + dataset.dataFileName()));
        return new TestPhysicalOperationProviders(testData.v1(), testData.v2());
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    private void assertWarnings(List<String> warnings) {
        List<String> normalized = new ArrayList<>(warnings.size());
        for (String w : warnings) {
            String normW = HeaderWarning.extractWarningValueFromWarningHeader(w, false);
            if (normW.startsWith("No limit defined, adding default limit of [") == false) {
                // too many tests do not have a LIMIT, we'll test this warning separately
                normalized.add(normW);
            }
        }
        EsqlTestUtils.assertWarnings(normalized, testCase.expectedWarnings(true), testCase.expectedWarningsRegex());
    }
}
