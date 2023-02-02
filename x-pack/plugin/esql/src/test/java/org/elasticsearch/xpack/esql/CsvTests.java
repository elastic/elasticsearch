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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils.Type;
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
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.CsvSpecReader;
import org.elasticsearch.xpack.ql.SpecReader;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.After;
import org.junit.Before;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.compute.operator.DriverRunner.runToCompletion;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.TEST_INDEX_SIMPLE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadPage;
import static org.elasticsearch.xpack.ql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.ql.TestUtils.classpathResources;
import static org.hamcrest.Matchers.greaterThan;

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
 */
public class CsvTests extends ESTestCase {

    private static final CsvPreference CSV_SPEC_PREFERENCES = new CsvPreference.Builder('"', '|', "\r\n").build();
    private static final String NULL_VALUE = "null";

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
    private final Mapper mapper = new Mapper();
    private final PhysicalPlanOptimizer physicalPlanOptimizer = new TestPhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
    private ThreadPool threadPool;

    private static IndexResolution loadIndexResolution() {
        var mapping = new TreeMap<String, EsField>(EsqlTestUtils.loadMapping("mapping-default.json"));
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
            assumeFalse("Test " + testName + " is not enabled", testName.endsWith("-Ignore"));
            doTest();
        } catch (Exception e) {
            throw reworkException(e);
        }
    }

    public void doTest() throws Throwable {
        Tuple<Page, List<String>> testData = loadPage(CsvTests.class.getResource("/employees.csv"));
        LocalExecutionPlanner planner = new LocalExecutionPlanner(
            BigArrays.NON_RECYCLING_INSTANCE,
            configuration,
            new TestPhysicalOperationProviders(testData.v1(), testData.v2())
        );

        ActualResults actualResults = getActualResults(planner);
        Tuple<List<Tuple<String, Type>>, List<List<Object>>> expected = expectedColumnsWithValues(testCase.expectedResults);

        assertThat(actualResults.colunmTypes.size(), greaterThan(0));

        for (Page p : actualResults.pages) {
            assertColumns(expected.v1(), p, actualResults.columnNames);
        }
        // TODO we'd like to assert the results of each page individually
        assertValues(expected.v2(), actualResults.pages, actualResults.colunmTypes);
    }

    private PhysicalPlan physicalPlan() {
        var parsed = parser.createStatement(testCase.query);
        var analyzed = analyzer.analyze(parsed);
        var logicalOptimized = logicalPlanOptimizer.optimize(analyzed);
        var physicalPlan = mapper.map(logicalOptimized);
        return physicalPlanOptimizer.optimize(physicalPlan);
    }

    record ActualResults(List<String> columnNames, List<DataType> colunmTypes, List<Page> pages) {}

    private ActualResults getActualResults(LocalExecutionPlanner planner) {
        PhysicalPlan physicalPlan = physicalPlan();
        List<Driver> drivers = new ArrayList<>();
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        List<String> columnNames = Expressions.names(physicalPlan.output());
        List<DataType> columnTypes = physicalPlan.output().stream().map(Expression::dataType).toList();
        try {
            LocalExecutionPlan localExecutionPlan = planner.plan(new OutputExec(physicalPlan, (l, p) -> { collectedPages.add(p); }));
            drivers.addAll(localExecutionPlan.createDrivers());

            runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
        } finally {
            Releasables.close(drivers);
        }
        return new ActualResults(columnNames, columnTypes, collectedPages);
    }

    private void assertColumns(List<Tuple<String, Type>> expectedColumns, Page actualResultsPage, List<String> columnNames) {
        assertEquals(
            format(null, "Unexpected number of columns; expected [{}] but actual was [{}]", expectedColumns.size(), columnNames.size()),
            expectedColumns.size(),
            columnNames.size()
        );
        List<Tuple<String, Type>> actualColumns = extractColumnsFromPage(
            actualResultsPage,
            columnNames,
            expectedColumns.stream().map(Tuple::v2).collect(Collectors.toList())
        );

        for (int i = 0; i < expectedColumns.size(); i++) {
            assertEquals(expectedColumns.get(i).v1(), actualColumns.get(i).v1());
            Type expectedType = expectedColumns.get(i).v2();
            // a returned Page can have a Block of a NULL type, whereas the type checked in the csv-spec cannot be null
            if (expectedType != null && expectedType != Type.NULL) {
                assertEquals("incorrect type for [" + expectedColumns.get(i).v1() + "]", expectedType, actualColumns.get(i).v2());
            }
        }
    }

    private List<Tuple<String, Type>> extractColumnsFromPage(Page page, List<String> columnNames, List<Type> expectedTypes) {
        var blockCount = page.getBlockCount();
        List<Tuple<String, Type>> result = new ArrayList<>(blockCount);
        for (int i = 0; i < blockCount; i++) {
            Block block = page.getBlock(i);
            result.add(new Tuple<>(columnNames.get(i), Type.asType(block.elementType(), expectedTypes.get(i))));
        }
        return result;
    }

    private void assertValues(List<List<Object>> expectedValues, List<Page> actualResultsPages, List<DataType> columnTypes) {
        var expectedRoWsCount = expectedValues.size();
        var actualRowsCount = actualResultsPages.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(
            format(null, "Unexpected number of rows; expected [{}] but actual was [{}]", expectedRoWsCount, actualRowsCount),
            expectedRoWsCount,
            actualRowsCount
        );

        assertEquals(expectedValues, TransportEsqlQueryAction.pagesToValues(columnTypes, actualResultsPages));
    }

    private Tuple<List<Tuple<String, Type>>, List<List<Object>>> expectedColumnsWithValues(String csv) {
        try (CsvListReader listReader = new CsvListReader(new StringReader(csv), CSV_SPEC_PREFERENCES)) {
            String[] header = listReader.getHeader(true);
            List<Tuple<String, Type>> columns = Arrays.stream(header).map(c -> {
                String[] nameWithType = c.split(":");
                String typeName = nameWithType[1].trim();
                if (typeName.length() == 0) {
                    throw new IllegalArgumentException("A type is always expected in the csv file; found " + nameWithType);
                }
                String name = nameWithType[0].trim();
                Type type = Type.asType(typeName);
                return Tuple.tuple(name, type);
            }).toList();

            List<List<Object>> values = new LinkedList<>();
            List<String> row;
            while ((row = listReader.read()) != null) {
                List<Object> rowValues = new ArrayList<>(row.size());
                for (int i = 0; i < row.size(); i++) {
                    String value = row.get(i);
                    if (value != null) {
                        value = value.trim();
                        if (value.equalsIgnoreCase(NULL_VALUE)) {
                            value = null;
                        }
                    }
                    Type type = columns.get(i).v2();
                    Object val = type == Type.DATE ? value : type.convert(value);
                    rowValues.add(val);
                }
                values.add(rowValues);
            }

            return Tuple.tuple(columns, values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
