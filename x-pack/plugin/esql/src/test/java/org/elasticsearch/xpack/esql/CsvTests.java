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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestLocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ChangePointExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TestPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.stats.DisabledSearchStats;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadPageFromCsv;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * CSV-based unit testing.
 * <p>
 * Queries and their result live *.csv-spec files.
 * The results used in these files were manually added by running the same query on a real (debug mode) ES node. CsvTestsDataLoader loads
 * the test data helping to get the said results.
 * <p>
 * {@link CsvTestsDataLoader} creates an index using the mapping in mapping-default.json. The same mapping file is also used to create the
 * IndexResolver that helps validate the correctness of the query and the supported field data types.
 * The created index and this class uses the data from employees.csv file as data. This class is creating one Page with Blocks in it using
 * this file and the type of blocks matches the type of the schema specified on the first line of the csv file. These being said, the
 * mapping in mapping-default.csv and employees.csv should be more or less in sync. An exception to this rule:
 * <p>
 * languages:integer,languages.long:long. The mapping has "long" as a sub-field of "languages". ES knows what to do with sub-field, but
 * employees.csv is specifically defining "languages.long" as "long" and also has duplicated columns for these two.
 * <p>
 * ATM the first line from employees.csv file is not synchronized with the mapping itself.
 * <p>
 * When we add support for more field types, CsvTests should change to support the new Block types. Same goes for employees.csv file
 * (the schema needs adjustment) and the mapping-default.json file (to add or change an existing field).
 * When we add more operators, optimization rules to the logical or physical plan optimizers, there may be the need to change the operators
 * in TestPhysicalOperationProviders or adjust TestPhysicalPlanOptimizer. For example, the TestPhysicalPlanOptimizer is skipping any
 * rules that push operations to ES itself (a Limit for example). The TestPhysicalOperationProviders is a bit more complicated than that:
 * it’s creating its own Source physical operator, aggregation operator (just a tiny bit of it) and field extract operator.
 * <p>
 * To log the results logResults() should return "true".
 * <p>
 * This test never pushes to Lucene because there isn't a Lucene index to push to. It always runs everything in
 * the compute engine. This yields the same results modulo a few things:
 * <ul>
 *     <li>Warnings for multivalued fields: See {@link SingleValueMatchQuery} for an in depth discussion, but the
 *         short version is this class will always emit warnings on multivalued fields but tests that run against
 *         a real index are only guaranteed to emit a warning if the document would match all filters <strong>except</strong>
 *         it has a multivalue field.</li>
 *     <li>Sorting: This class emits values in the order they appear in the {@code .csv} files that power it. A real
 *         index emits documents a fair random order. Multi-shard and multi-node tests doubly so.</li>
 * </ul>
 */
// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class CsvTests extends ESTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CsvTests.class);

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvSpecReader.CsvTestCase testCase;
    private final String instructions;

    private final Configuration configuration = EsqlTestUtils.configuration(
        new QueryPragmas(Settings.builder().put("page_size", randomPageSize()).build())
    );
    private final EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    private final EsqlParser parser = new EsqlParser();
    private final Mapper mapper = new Mapper();
    private ThreadPool threadPool;
    private Executor executor;

    @ParametersFactory(argumentFormatting = "%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertThat("Not enough specs found " + urls, urls, hasSize(greaterThan(0)));
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
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

    private int randomPageSize() {
        if (randomBoolean()) {
            return between(1, 16);
        } else {
            return between(1, 16 * 1024);
        }
    }

    public CsvTests(
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

    public final void test() throws Throwable {
        try {
            assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
            /*
             * The csv tests support all but a few features. The unsupported features
             * are tested in integration tests.
             */
            assumeFalse(
                "metadata fields aren't supported",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_FIELDS.capabilityName())
            );
            assumeFalse(
                "enrich can't load fields in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.ENRICH_LOAD.capabilityName())
            );
            assumeFalse(
                "can't use rereank in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.RERANK.capabilityName())
            );
            assumeFalse(
                "can't use completion in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.COMPLETION.capabilityName())
            );
            assumeFalse(
                "can't use match in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.MATCH_OPERATOR_COLON.capabilityName())
            );
            assumeFalse(
                "can't load metrics in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METRICS_COMMAND.capabilityName())
            );
            assumeFalse(
                "can't use QSTR function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.QSTR_FUNCTION.capabilityName())
            );
            assumeFalse(
                "can't use MATCH function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.MATCH_FUNCTION.capabilityName())
            );
            assumeFalse(
                "can't use MATCH_PHRASE function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.MATCH_PHRASE_FUNCTION.capabilityName())
            );
            assumeFalse(
                "can't use KQL function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.KQL_FUNCTION.capabilityName())
            );
            assumeFalse(
                "can't use KNN function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.KNN_FUNCTION.capabilityName())
            );
            assumeFalse(
                "lookup join disabled for csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName())
            );
            assumeFalse(
                "can't use TERM function in csv tests",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.TERM_FUNCTION.capabilityName())
            );
            assumeFalse(
                "CSV tests cannot correctly handle the field caps change",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS.capabilityName())
            );
            assumeFalse(
                "CSV tests cannot currently handle the _source field mapping directives",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.SOURCE_FIELD_MAPPING.capabilityName())
            );
            assumeFalse(
                "CSV tests cannot currently handle scoring that depends on Lucene",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_SCORE.capabilityName())
            );
            assumeFalse(
                "CSV tests cannot currently handle FORK",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.FORK_V6.capabilityName())
            );
            assumeFalse(
                "CSV tests cannot currently handle multi_match function that depends on Lucene",
                testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.capabilityName())
            );

            if (Build.current().isSnapshot()) {
                assertThat(
                    "Capability is not included in the enabled list capabilities on a snapshot build. Spelling mistake?",
                    testCase.requiredCapabilities,
                    everyItem(in(EsqlCapabilities.capabilities(true)))
                );
                assumeTrue(
                    "Capability not supported in this build",
                    EsqlCapabilities.capabilities(false).containsAll(testCase.requiredCapabilities)
                );
            } else {
                for (EsqlCapabilities.Cap c : EsqlCapabilities.Cap.values()) {
                    if (false == c.isEnabled()) {
                        assumeFalse(
                            c.capabilityName() + " is not supported in non-snapshot releases",
                            testCase.requiredCapabilities.contains(c.capabilityName())
                        );
                    }
                }
            }

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
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        var actualResults = executePlan(bigArrays);
        try {
            var expected = loadCsvSpecValues(testCase.expectedResults);

            var log = logResults() ? LOGGER : null;
            assertResults(expected, actualResults, testCase.ignoreOrder, log);
            assertWarnings(actualResults.responseHeaders().getOrDefault("Warning", List.of()));
        } finally {
            Releasables.close(() -> Iterators.map(actualResults.pages().iterator(), p -> p::releaseBlocks));
            // Give the breaker service some time to clear in case we got results before the rest of the driver had cleaned up
            assertBusy(
                () -> assertThat(
                    "Not all circuits were cleaned up",
                    bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(),
                    equalTo(0L)
                )
            );
        }
    }

    protected void assertResults(ExpectedResults expected, ActualResults actual, boolean ignoreOrder, Logger logger) {
        /*
         * Enable the next two lines to see the results returned by ES.
         * This is useful when creating a new test or trying to figure out what are the actual results.
         */
        // CsvTestUtils.logMetaData(actual.columnNames(), actual.columnTypes(), LOGGER);
        // CsvTestUtils.logData(actual.values(), LOGGER);

        CsvAssert.assertResults(expected, actual, ignoreOrder, logger);
    }

    private static IndexResolution loadIndexResolution(CsvTestsDataLoader.MultiIndexTestDataset datasets) {
        var indexNames = datasets.datasets().stream().map(CsvTestsDataLoader.TestDataset::indexName);
        Map<String, IndexMode> indexModes = indexNames.collect(Collectors.toMap(x -> x, x -> IndexMode.STANDARD));
        List<MappingPerIndex> mappings = datasets.datasets()
            .stream()
            .map(ds -> new MappingPerIndex(ds.indexName(), createMappingForIndex(ds)))
            .toList();
        var mergedMappings = mergeMappings(mappings);
        return IndexResolution.valid(
            new EsIndex(datasets.indexPattern(), mergedMappings.mapping, indexModes, mergedMappings.partiallyUnmappedFields)
        );
    }

    private static Map<String, EsField> createMappingForIndex(CsvTestsDataLoader.TestDataset dataset) {
        var mapping = new TreeMap<>(loadMapping(dataset.mappingFileName()));
        if (dataset.typeMapping() == null) {
            return mapping;
        }
        for (var entry : dataset.typeMapping().entrySet()) {
            if (mapping.containsKey(entry.getKey())) {
                DataType dataType = DataType.fromTypeName(entry.getValue());
                EsField field = mapping.get(entry.getKey());
                EsField editedField = new EsField(field.getName(), dataType, field.getProperties(), field.isAggregatable());
                mapping.put(entry.getKey(), editedField);
            }
        }
        return mapping;
    }

    record MappingPerIndex(String index, Map<String, EsField> mapping) {}

    record MergedResult(Map<String, EsField> mapping, Set<String> partiallyUnmappedFields) {}

    private static MergedResult mergeMappings(List<MappingPerIndex> mappingsPerIndex) {
        int numberOfIndices = mappingsPerIndex.size();
        Map<String, Map<String, EsField>> columnNamesToFieldByIndices = new HashMap<>();
        for (var mappingPerIndex : mappingsPerIndex) {
            for (var entry : mappingPerIndex.mapping().entrySet()) {
                String columnName = entry.getKey();
                EsField field = entry.getValue();
                columnNamesToFieldByIndices.computeIfAbsent(columnName, k -> new HashMap<>()).put(mappingPerIndex.index(), field);
            }
        }

        var partiallyUnmappedFields = columnNamesToFieldByIndices.entrySet()
            .stream()
            .filter(e -> e.getValue().size() < numberOfIndices)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        var mappings = columnNamesToFieldByIndices.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> mergeFields(e.getKey(), e.getValue())));
        return new MergedResult(mappings, partiallyUnmappedFields);
    }

    private static EsField mergeFields(String index, Map<String, EsField> columnNameToField) {
        var indexFields = columnNameToField.values();
        if (indexFields.stream().distinct().count() > 1) {
            var typesToIndices = new HashMap<String, Set<String>>();
            for (var typeToIndex : columnNameToField.entrySet()) {
                typesToIndices.computeIfAbsent(typeToIndex.getValue().getDataType().typeName(), k -> new HashSet<>())
                    .add(typeToIndex.getKey());
            }
            return new InvalidMappedField(index, typesToIndices);
        } else {
            return indexFields.iterator().next();
        }
    }

    private static EnrichResolution loadEnrichPolicies() {
        EnrichResolution enrichResolution = new EnrichResolution();
        for (CsvTestsDataLoader.EnrichConfig policyConfig : CsvTestsDataLoader.ENRICH_POLICIES) {
            EnrichPolicy policy = loadEnrichPolicyMapping(policyConfig.policyFileName());
            CsvTestsDataLoader.TestDataset sourceIndex = CSV_DATASET_MAP.get(policy.getIndices().get(0));
            // this could practically work, but it's wrong:
            // EnrichPolicyResolution should contain the policy (system) index, not the source index
            EsIndex esIndex = loadIndexResolution(CsvTestsDataLoader.MultiIndexTestDataset.of(sourceIndex.withTypeMapping(Map.of()))).get();
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

    private LogicalPlan analyzedPlan(LogicalPlan parsed, CsvTestsDataLoader.MultiIndexTestDataset datasets) {
        var indexResolution = loadIndexResolution(datasets);
        var enrichPolicies = loadEnrichPolicies();
        var analyzer = new Analyzer(
            new AnalyzerContext(configuration, functionRegistry, indexResolution, enrichPolicies, emptyInferenceResolution()),
            TEST_VERIFIER
        );
        LogicalPlan plan = analyzer.analyze(parsed);
        plan.setAnalyzed();
        LOGGER.debug("Analyzed plan:\n{}", plan);
        return plan;
    }

    private static CsvTestsDataLoader.MultiIndexTestDataset testDatasets(LogicalPlan parsed) {
        var preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        var indices = preAnalysis.indices;
        if (indices.isEmpty()) {
            /*
             * If the data set doesn't matter we'll just grab one we know works.
             * Employees is fine.
             */
            return CsvTestsDataLoader.MultiIndexTestDataset.of(CSV_DATASET_MAP.get("employees"));
        } else if (preAnalysis.indices.size() > 1) {
            throw new IllegalArgumentException("unexpected index resolution to multiple entries [" + preAnalysis.indices.size() + "]");
        }

        String indexName = indices.getFirst().indexPattern();
        List<CsvTestsDataLoader.TestDataset> datasets = new ArrayList<>();
        if (indexName.endsWith("*")) {
            String indexPrefix = indexName.substring(0, indexName.length() - 1);
            for (var entry : CSV_DATASET_MAP.entrySet()) {
                if (entry.getKey().startsWith(indexPrefix)) {
                    datasets.add(entry.getValue());
                }
            }
        } else {
            for (String index : indexName.split(",")) {
                var dataset = CSV_DATASET_MAP.get(index);
                if (dataset == null) {
                    throw new IllegalArgumentException("unknown CSV dataset for table [" + index + "]");
                }
                datasets.add(dataset);
            }
        }
        if (datasets.isEmpty()) {
            throw new IllegalArgumentException("unknown CSV dataset for table [" + indexName + "]");
        }
        return new CsvTestsDataLoader.MultiIndexTestDataset(indexName, datasets);
    }

    private static TestPhysicalOperationProviders testOperationProviders(
        FoldContext foldCtx,
        CsvTestsDataLoader.MultiIndexTestDataset datasets
    ) throws Exception {
        var indexPages = new ArrayList<TestPhysicalOperationProviders.IndexPage>();
        for (CsvTestsDataLoader.TestDataset dataset : datasets.datasets()) {
            var testData = loadPageFromCsv(CsvTests.class.getResource("/data/" + dataset.dataFileName()), dataset.typeMapping());
            Set<String> mappedFields = loadMapping(dataset.mappingFileName()).keySet();
            indexPages.add(new TestPhysicalOperationProviders.IndexPage(dataset.indexName(), testData.v1(), testData.v2(), mappedFields));
        }
        return TestPhysicalOperationProviders.create(foldCtx, indexPages);
    }

    private ActualResults executePlan(BigArrays bigArrays) throws Exception {
        LogicalPlan parsed = parser.createStatement(testCase.query);
        var testDatasets = testDatasets(parsed);
        LogicalPlan analyzed = analyzedPlan(parsed, testDatasets);

        FoldContext foldCtx = FoldContext.small();
        EsqlSession session = new EsqlSession(
            getTestName(),
            configuration,
            null,
            null,
            null,
            functionRegistry,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(configuration, foldCtx)),
            mapper,
            TEST_VERIFIER,
            new PlanTelemetry(functionRegistry),
            null,
            EsqlTestUtils.MOCK_TRANSPORT_ACTION_SERVICES
        );
        TestPhysicalOperationProviders physicalOperationProviders = testOperationProviders(foldCtx, testDatasets);

        PlainActionFuture<ActualResults> listener = new PlainActionFuture<>();

        session.executeOptimizedPlan(
            new EsqlQueryRequest(),
            new EsqlExecutionInfo(randomBoolean()),
            planRunner(bigArrays, foldCtx, physicalOperationProviders),
            session.optimizedPlan(analyzed),
            listener.delegateFailureAndWrap(
                // Wrap so we can capture the warnings in the calling thread
                (next, result) -> next.onResponse(
                    new ActualResults(
                        result.schema().stream().map(Attribute::name).toList(),
                        result.schema().stream().map(a -> Type.asType(a.dataType().nameUpper())).toList(),
                        result.schema().stream().map(Attribute::dataType).toList(),
                        result.pages(),
                        threadPool.getThreadContext().getResponseHeaders()
                    )
                )
            )
        );
        return listener.get();
    }

    private Settings randomNodeSettings() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofBytes(randomIntBetween(0, 4096)));
            builder.put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofBytes(randomIntBetween(0, 16 * 1024)));
        }
        return builder.build();
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    // Asserts that the serialization and deserialization of the plan creates an equivalent plan.
    private void opportunisticallyAssertPlanSerialization(PhysicalPlan plan) {
        if (plan.anyMatch(
            p -> p instanceof LocalSourceExec || p instanceof HashJoinExec || p instanceof ChangePointExec || p instanceof MergeExec
        )) {
            return;
        }
        SerializationTestUtils.assertSerialization(plan, configuration);
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
        testCase.assertWarnings(false).assertWarnings(normalized);
    }

    PlanRunner planRunner(BigArrays bigArrays, FoldContext foldCtx, TestPhysicalOperationProviders physicalOperationProviders) {
        return (physicalPlan, listener) -> executeSubPlan(bigArrays, foldCtx, physicalOperationProviders, physicalPlan, listener);
    }

    void executeSubPlan(
        BigArrays bigArrays,
        FoldContext foldCtx,
        TestPhysicalOperationProviders physicalOperationProviders,
        PhysicalPlan physicalPlan,
        ActionListener<Result> listener
    ) {
        // Keep in sync with ComputeService#execute
        opportunisticallyAssertPlanSerialization(physicalPlan);
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            physicalPlan,
            configuration
        );
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();
        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Coordinator plan\n" + coordinatorPlan);
            LOGGER.trace("DataNode plan\n" + dataNodePlan);
        }

        BlockFactory blockFactory = new BlockFactory(
            bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST),
            bigArrays,
            ByteSizeValue.ofBytes(randomLongBetween(1, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes() * 2))
        );
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(between(1, 64), executor);
        ExchangeSinkHandler exchangeSink = new ExchangeSinkHandler(blockFactory, between(1, 64), threadPool::relativeTimeInMillis);

        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
            getTestName(),
            "",
            new CancellableTask(1, "transport", "esql", null, TaskId.EMPTY_TASK_ID, Map.of()),
            bigArrays,
            blockFactory,
            randomNodeSettings(),
            configuration,
            exchangeSource::createExchangeSource,
            () -> exchangeSink.createExchangeSink(() -> {}),
            Mockito.mock(EnrichLookupService.class),
            Mockito.mock(LookupFromIndexService.class),
            Mockito.mock(InferenceRunner.class),
            physicalOperationProviders,
            List.of()
        );

        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // replace fragment inside the coordinator plan
        List<Driver> drivers = new ArrayList<>();
        LocalExecutionPlan coordinatorNodeExecutionPlan = executionPlanner.plan(
            "final",
            foldCtx,
            new OutputExec(coordinatorPlan, collectedPages::add)
        );
        drivers.addAll(coordinatorNodeExecutionPlan.createDrivers(getTestName()));
        if (dataNodePlan != null) {
            var searchStats = new DisabledSearchStats();
            var logicalTestOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(configuration, foldCtx, searchStats));
            var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
                new LocalPhysicalOptimizerContext(configuration, foldCtx, searchStats)
            );

            var csvDataNodePhysicalPlan = PlannerUtils.localPlan(dataNodePlan, logicalTestOptimizer, physicalTestOptimizer);
            exchangeSource.addRemoteSink(
                exchangeSink::fetchPageAsync,
                Randomness.get().nextBoolean(),
                () -> {},
                randomIntBetween(1, 3),
                ActionListener.<Void>noop().delegateResponse((l, e) -> {
                    throw new AssertionError("expected no failure", e);
                })
            );
            LocalExecutionPlan dataNodeExecutionPlan = executionPlanner.plan("data", foldCtx, csvDataNodePhysicalPlan);

            drivers.addAll(dataNodeExecutionPlan.createDrivers(getTestName()));
            Randomness.shuffle(drivers);
        }
        // Execute the drivers
        DriverRunner runner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.getThreadContext(), executor, driver, between(1, 1000), driverListener);
            }
        };
        listener = ActionListener.releaseAfter(listener, () -> Releasables.close(drivers));
        runner.runToCompletion(
            drivers,
            listener.map(ignore -> new Result(physicalPlan.output(), collectedPages, DriverCompletionInfo.EMPTY, null))
        );
    }
}
