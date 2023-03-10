/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.TestPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.CsvSpecReader;
import org.elasticsearch.xpack.ql.SpecReader;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.After;
import org.junit.Before;

import java.net.URL;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.compute.operator.DriverRunner.runToCompletion;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvValues;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadPage;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.TEST_INDEX_SIMPLE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;

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
 * ATM the first line from employees.csv file is not synchronized with the mapping itself, mainly because atm we do not support certain data
 * types (still_hired field should be “boolean”, birth_date and hire_date should be “date” fields).
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
public class CsvTests extends ESTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CsvTests.class);

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvSpecReader.CsvTestCase testCase;
    private IndexResolution indexResolution = loadIndexResolution();
    private final EsqlConfiguration configuration = new EsqlConfiguration(
        ZoneOffset.UTC,
        null,
        null,
        Settings.EMPTY,
        EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY)
    );
    private final FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    private final EsqlParser parser = new EsqlParser();
    private final Analyzer analyzer = new Analyzer(new AnalyzerContext(configuration, functionRegistry, indexResolution), new Verifier());
    private final LogicalPlanOptimizer logicalPlanOptimizer = new LogicalPlanOptimizer();
    private final Mapper mapper = new Mapper(functionRegistry);
    private final PhysicalPlanOptimizer physicalPlanOptimizer = new TestPhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
    private ThreadPool threadPool;

    private static IndexResolution loadIndexResolution() {
        var mapping = new TreeMap<String, EsField>(loadMapping(CsvTestsDataLoader.MAPPING));
        return IndexResolution.valid(new EsIndex(TEST_INDEX_SIMPLE, mapping));
    }

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("CsvTests");
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

    public boolean logResults() {
        return false;
    }

    public void doTest() throws Throwable {
        Tuple<Page, List<String>> testData = loadPage(CsvTests.class.getResource("/" + CsvTestsDataLoader.DATA));
        LocalExecutionPlanner planner = new LocalExecutionPlanner(
            BigArrays.NON_RECYCLING_INSTANCE,
            configuration,
            new TestPhysicalOperationProviders(testData.v1(), testData.v2())
        );

        var actualResults = executePlan(planner);
        var expected = loadCsvValues(testCase.expectedResults);

        var log = logResults() ? LOGGER : null;
        assertResults(expected, actualResults, log);
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

    private PhysicalPlan physicalPlan() {
        var parsed = parser.createStatement(testCase.query);
        var analyzed = analyzer.analyze(parsed);
        var logicalOptimized = logicalPlanOptimizer.optimize(analyzed);
        var physicalPlan = mapper.map(logicalOptimized);
        return physicalPlanOptimizer.optimize(physicalPlan);
    }

    private ActualResults executePlan(LocalExecutionPlanner planner) {
        PhysicalPlan physicalPlan = physicalPlan();
        List<Driver> drivers = new ArrayList<>();
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        List<String> columnNames = Expressions.names(physicalPlan.output());
        List<DataType> dataTypes = new ArrayList<>(columnNames.size());
        List<Type> columnTypes = physicalPlan.output()
            .stream()
            .peek(o -> dataTypes.add(o.dataType()))
            .map(o -> Type.asType(o.dataType().name()))
            .toList();
        try {
            LocalExecutionPlan localExecutionPlan = planner.plan(new OutputExec(physicalPlan, (l, p) -> { collectedPages.add(p); }));
            drivers.addAll(localExecutionPlan.createDrivers("csv-test-session"));

            runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
        } finally {
            Releasables.close(drivers);
        }
        return new ActualResults(columnNames, columnTypes, dataTypes, collectedPages);
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }
}
