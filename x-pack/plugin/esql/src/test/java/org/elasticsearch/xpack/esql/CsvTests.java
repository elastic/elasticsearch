/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestLocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.TestPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.CsvSpecReader;
import org.elasticsearch.xpack.ql.SpecReader;
import org.elasticsearch.xpack.ql.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.util.DateUtils;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.compute.operator.DriverRunner.runToCompletion;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadPageFromCsv;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;
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
 * When we add more operators, optimization rules to the logical or physical plan optimizers, there may be the need to change the operators
 * in TestPhysicalOperationProviders or adjust TestPhysicalPlanOptimizer. For example, the TestPhysicalPlanOptimizer is skipping any
 * rules that push operations to ES itself (a Limit for example). The TestPhysicalOperationProviders is a bit more complicated than that:
 * it’s creating its own Source physical operator, aggregation operator (just a tiny bit of it) and field extract operator.
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

    private final EsqlConfiguration configuration = new EsqlConfiguration(
        ZoneOffset.UTC,
        null,
        null,
        new QueryPragmas(Settings.EMPTY),
        EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY)
    );
    private final FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    private final EsqlParser parser = new EsqlParser();
    private final LogicalPlanOptimizer logicalPlanOptimizer = new LogicalPlanOptimizer();
    private final Mapper mapper = new Mapper(functionRegistry);
    private final PhysicalPlanOptimizer physicalPlanOptimizer = new TestPhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
    private ThreadPool threadPool;

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec").stream().filter(x -> x.toString().contains("-ignoreCsvTests") == false).toList();
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "CsvTests",
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_THREAD_POOL_NAME, numThreads, 1024, "esql", false)
        );
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
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
            assumeTrue("Test " + testName + " is not enabled", isEnabled(testName));
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
        var actualResults = executePlan();
        var expected = loadCsvSpecValues(testCase.expectedResults);

        var log = logResults() ? LOGGER : null;
        assertResults(expected, actualResults, log);
        assertWarnings(actualResults.responseHeaders().getOrDefault("Warning", List.of()));
    }

    protected void assertResults(ExpectedResults expected, ActualResults actual, Logger logger) {
        CsvAssert.assertResults(expected, actual, logger);
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
        Set<String> names = new HashSet<>();
        Set<EnrichPolicyResolution> resolutions = new HashSet<>();
        for (CsvTestsDataLoader.EnrichConfig policyConfig : CsvTestsDataLoader.ENRICH_POLICIES) {
            EnrichPolicy policy = loadEnrichPolicyMapping(policyConfig.policyFileName());
            CsvTestsDataLoader.TestsDataset sourceIndex = CSV_DATASET_MAP.get(policy.getIndices().get(0));
            // this could practically work, but it's wrong:
            // EnrichPolicyResolution should contain the policy (system) index, not the source index
            IndexResolution idxRes = loadIndexResolution(sourceIndex.mappingFileName(), sourceIndex.indexName());
            names.add(policyConfig.policyName());
            resolutions.add(new EnrichPolicyResolution(policyConfig.policyName(), policy, idxRes));
        }
        return new EnrichResolution(resolutions, names);
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

    private PhysicalPlan physicalPlan(LogicalPlan parsed, CsvTestsDataLoader.TestsDataset dataset) {
        var indexResolution = loadIndexResolution(dataset.mappingFileName(), dataset.indexName());
        var enrichPolicies = loadEnrichPolicies();
        var analyzer = new Analyzer(
            new AnalyzerContext(configuration, functionRegistry, indexResolution, enrichPolicies),
            new Verifier(new Metrics())
        );
        var analyzed = analyzer.analyze(parsed);
        var logicalOptimized = logicalPlanOptimizer.optimize(analyzed);
        var physicalPlan = mapper.map(logicalOptimized);
        var optimizedPlan = physicalPlanOptimizer.optimize(physicalPlan);
        opportunisticallyAssertPlanSerialization(physicalPlan, optimizedPlan); // comment out to disable serialization
        return optimizedPlan;
    }

    private static CsvTestsDataLoader.TestsDataset testsDataset(LogicalPlan parsed) {
        var preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        var indices = preAnalysis.indices;
        if (indices.size() == 0) {
            return CSV_DATASET_MAP.values().iterator().next(); // default dataset for `row` source command
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

    private ActualResults executePlan() throws Exception {
        var parsed = parser.createStatement(testCase.query);
        var testDataset = testsDataset(parsed);

        ExchangeService exchangeService = new ExchangeService(Settings.EMPTY, threadPool);
        String sessionId = "csv-test";
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
            sessionId,
            new CancellableTask(1, "transport", "esql", null, TaskId.EMPTY_TASK_ID, Map.of()),
            BigArrays.NON_RECYCLING_INSTANCE,
            threadPool,
            configuration,
            exchangeService,
            Mockito.mock(EnrichLookupService.class),
            testOperationProviders(testDataset)
        );
        //
        // Keep in sync with ComputeService#execute
        //
        var localTestOptimizer = new TestLocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(configuration));

        PhysicalPlan physicalPlan = physicalPlan(parsed, testDataset);
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = CSVbreakPlanBetweenCoordinatorAndDataNode(
            physicalPlan,
            localTestOptimizer
        );
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();
        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();

        List<String> columnNames = Expressions.names(coordinatorPlan.output());
        List<String> dataTypes = new ArrayList<>(columnNames.size());
        List<Type> columnTypes = coordinatorPlan.output()
            .stream()
            .peek(o -> dataTypes.add(EsqlDataTypes.outputType(o.dataType())))
            .map(o -> Type.asType(o.dataType().name()))
            .toList();

        List<Driver> drivers = new ArrayList<>();
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        Map<String, List<String>> responseHeaders;

        ExchangeSourceHandler sourceHandler = exchangeService.createSourceHandler(
            sessionId,
            randomIntBetween(1, 64),
            ESQL_THREAD_POOL_NAME
        );
        // replace fragment inside the coordinator plan
        try {
            LocalExecutionPlan coordinatorNodeExecutionPlan = executionPlanner.plan(new OutputExec(coordinatorPlan, collectedPages::add));
            drivers.addAll(coordinatorNodeExecutionPlan.createDrivers(sessionId));
            if (dataNodePlan != null) {
                var csvDataNodePhysicalPlan = CSVlocalPlan(List.of(), configuration, dataNodePlan, localTestOptimizer);
                ExchangeSinkHandler sinkHandler = exchangeService.createSinkHandler(sessionId, randomIntBetween(1, 64));
                sourceHandler.addRemoteSink(sinkHandler::fetchPageAsync, randomIntBetween(1, 3));
                LocalExecutionPlan dataNodeExecutionPlan = executionPlanner.plan(csvDataNodePhysicalPlan);
                drivers.addAll(dataNodeExecutionPlan.createDrivers(sessionId));
            }
            responseHeaders = runToCompletion(threadPool, drivers);
        } finally {
            Releasables.close(
                () -> Releasables.close(drivers),
                () -> exchangeService.completeSinkHandler(sessionId),
                sourceHandler::decRef
            );
        }
        return new ActualResults(columnNames, columnTypes, dataTypes, collectedPages, responseHeaders);
    }

    //
    // Clone of PlannerUtils
    //

    private static Tuple<PhysicalPlan, PhysicalPlan> CSVbreakPlanBetweenCoordinatorAndDataNode(
        PhysicalPlan plan,
        LocalPhysicalPlanOptimizer optimizer
    ) {
        var dataNodePlan = new Holder<PhysicalPlan>();

        // split the given plan when encountering the exchange
        PhysicalPlan coordinatorPlan = plan.transformUp(ExchangeExec.class, e -> {
            // remember the datanode subplan and wire it to a sink
            var subplan = e.child();
            dataNodePlan.set(new ExchangeSinkExec(e.source(), subplan));

            // ugly hack to get the layout
            var dummyConfig = new EsqlConfiguration(DateUtils.UTC, StringUtils.EMPTY, StringUtils.EMPTY, QueryPragmas.EMPTY, 1000);
            var planContainingTheLayout = CSVlocalPlan(List.of(), dummyConfig, subplan, optimizer);
            // replace the subnode with an exchange source
            return new ExchangeSourceExec(e.source(), e.output(), planContainingTheLayout);
        });
        return new Tuple<>(coordinatorPlan, dataNodePlan.get());
    }

    private static PhysicalPlan CSVlocalPlan(
        List<SearchContext> searchContexts,
        EsqlConfiguration configuration,
        PhysicalPlan plan,
        LocalPhysicalPlanOptimizer optimizer
    ) {
        final Mapper mapper = new Mapper(true);

        var localPhysicalPlan = plan.transformUp(FragmentExec.class, f -> {
            var optimizedFragment = new LocalLogicalPlanOptimizer().localOptimize(f.fragment());
            var physicalFragment = mapper.map(optimizedFragment);
            return physicalFragment;
        });
        return optimizer.localOptimize(localPhysicalPlan);
    }

    //

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    // Asserts that the serialization and deserialization of the plan creates an equivalent plan.
    private static void opportunisticallyAssertPlanSerialization(PhysicalPlan... plans) {
        for (var plan : plans) {
            var tmp = plan;
            do {
                if (tmp instanceof LocalSourceExec) {
                    return; // skip plans with localSourceExec
                }
            } while (tmp.children().isEmpty() == false && (tmp = tmp.children().get(0)) != null);

            SerializationTestUtils.assertSerialization(plan);
        }
    }

    private void assertWarnings(List<String> warnings) {
        List<String> normalized = new ArrayList<>(warnings.size());
        for (String w : warnings) {
            normalized.add(HeaderWarning.extractWarningValueFromWarningHeader(w, false));
        }
        assertMap(normalized, matchesList(testCase.expectedWarnings));
    }
}
