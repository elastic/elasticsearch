/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
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
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.LookupIdxColumn;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
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
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.EsqlSession.PlanRunner;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.stats.DisabledSearchStats;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.junit.After;
import org.junit.Before;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadPageFromCsv;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET_MAP;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_POLICIES;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.availableDatasetsForEs;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class GenerativeCsvTests extends ESTestCase implements QueryExecutor {

    private static final Logger LOGGER = LogManager.getLogger(GenerativeCsvTests.class);

    public static final int ITERATIONS = 100;
    public static final int MAX_DEPTH = 20;

    public static final Set<String> ALLOWED_ERRORS = Set.of(
        "Reference \\[.*\\] is ambiguous",
        "Cannot use field \\[.*\\] due to ambiguities",
        "cannot sort on .*",
        "argument of \\[count.*\\] must",
        "Cannot use field \\[.*\\] with unsupported type \\[.*\\]",
        "Unbounded SORT not supported yet",
        "The field names are too complex to process", // field_caps problem
        "must be \\[any type except counter types\\]", // TODO refine the generation of count()

        // Awaiting fixes for query failure
        "Unknown column \\[<all-fields-projected>\\]", // https://github.com/elastic/elasticsearch/issues/121741,
        // https://github.com/elastic/elasticsearch/issues/125866
        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references",
        "The incoming YAML document exceeds the limit:", // still to investigate, but it seems to be specific to the test framework
        "Data too large", // Circuit breaker exceptions eg. https://github.com/elastic/elasticsearch/issues/130072
        "optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/131509

        // Awaiting fixes for correctness
        "Expecting at most \\[.*\\] columns, got \\[.*\\]" // https://github.com/elastic/elasticsearch/issues/129561
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    private final Configuration configuration = EsqlTestUtils.configuration(
        new QueryPragmas(Settings.builder().put("page_size", randomPageSize()).build())
    );
    private final EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    private final EsqlParser parser = new EsqlParser();
    private final Mapper mapper = new Mapper();
    private ThreadPool threadPool;
    private Executor executor;

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

    public GenerativeCsvTests() {}

    @Override
    protected final boolean enableWarningsCheck() {
        return false;  // We use our own warnings check
    }

    public Set<String> excludedDatasets() {
        // these have subfields, ranges and other things that are not properly supported in CSV
        return Set.of(
            "system_metrics",
            "mv_text",
            "multi_column_joinable",
            "heights",
            "firewall_logs",
            "app_logs",
            "employees_incompatible",
            "employees_incompatible_types",
            "decades",
            "k8s",
            "k8s-downsampled",
            "ages",
            "books"
        );
    }

    public void test() throws Exception {
        List<String> indices = availableIndices().stream().filter(x -> excludedDatasets().contains(x) == false).toList();
        List<LookupIdx> lookupIndices = lookupIndices();
        List<CsvTestsDataLoader.EnrichConfig> policies = availableEnrichPolicies();
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(indices, lookupIndices, policies);

        for (int i = 0; i < ITERATIONS; i++) {
            var exec = new EsqlQueryGenerator.Executor() {
                @Override
                public void run(CommandGenerator generator, CommandGenerator.CommandDescription current) {
                    previousCommands.add(current);
                    final String command = current.commandString();

                    final QueryExecuted result = previousResult == null
                        ? execute(command, 0)
                        : execute(previousResult.query() + command, previousResult.depth());
                    previousResult = result;

                    logger.trace(() -> "query:" + result.query());

                    final boolean hasException = result.exception() != null;
                    if (hasException || checkResults(List.of(), generator, current, previousResult, result).success() == false) {
                        if (hasException) {
                            checkException(result);
                        }
                        continueExecuting = false;
                        currentSchema = List.of();
                    } else {
                        continueExecuting = true;
                        currentSchema = result.outputSchema();
                    }
                }

                @Override
                public List<CommandGenerator.CommandDescription> previousCommands() {
                    return previousCommands;
                }

                @Override
                public boolean continueExecuting() {
                    return continueExecuting;
                }

                @Override
                public List<Column> currentSchema() {
                    return currentSchema;
                }

                boolean continueExecuting;
                List<Column> currentSchema;
                final List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
                QueryExecuted previousResult;
            };
            EsqlQueryGenerator.generatePipeline(
                MAX_DEPTH,
                EsqlQueryGenerator.simplifiedSourceCommand(),
                EsqlQueryGenerator.SIMPLIFIED_PIPE_COMMANDS,
                mappingInfo,
                exec,
                this
            );
        }
    }

    private static CommandGenerator.ValidationResult checkResults(
        List<CommandGenerator.CommandDescription> previousCommands,
        CommandGenerator commandGenerator,
        CommandGenerator.CommandDescription commandDescription,
        QueryExecuted previousResult,
        QueryExecuted result
    ) {
        CommandGenerator.ValidationResult outputValidation = commandGenerator.validateOutput(
            previousCommands,
            commandDescription,
            previousResult == null ? null : previousResult.outputSchema(),
            previousResult == null ? null : previousResult.result(),
            result.outputSchema(),
            result.result(),
            true
        );
        if (outputValidation.success() == false) {
            for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
                if (isAllowedError(outputValidation.errorMessage(), allowedError)) {
                    return outputValidation;
                }
            }
            fail("query: " + result.query() + "\nerror: " + outputValidation.errorMessage());
        }
        return outputValidation;
    }

    private void checkException(QueryExecuted query) {
        for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
            if (isAllowedError(query.exception().getMessage(), allowedError)) {
                return;
            }
        }
        fail("query: " + query.query() + "\nexception: " + query.exception().getMessage());
    }

    /**
     * Long lines in exceptions can be split across several lines. When a newline is inserted, the end of the current line and the beginning
     * of the new line are marked with a backslash {@code \}; the new line will also have whitespace before the backslash for aligning.
     */
    private static final Pattern ERROR_MESSAGE_LINE_BREAK = Pattern.compile("\\\\\n\\s*\\\\");

    private static boolean isAllowedError(String errorMessage, Pattern allowedPattern) {
        String errorWithoutLineBreaks = ERROR_MESSAGE_LINE_BREAK.matcher(errorMessage).replaceAll("");
        return allowedPattern.matcher(errorWithoutLineBreaks).matches();
    }

    private List<String> availableIndices() throws IOException {
        return availableDatasetsForEs(false, false, false).stream()
            .filter(x -> x.requiresInferenceEndpoint() == false)
            .map(x -> x.indexName())
            .toList();
    }

    private List<LookupIdx> lookupIndices() {
        List<LookupIdx> result = new ArrayList<>();
        // we don't have key info from the dataset loader, let's hardcode it for now
        result.add(new LookupIdx("languages_lookup", List.of(new LookupIdxColumn("language_code", "integer"))));
        result.add(new LookupIdx("message_types_lookup", List.of(new LookupIdxColumn("message", "keyword"))));
        List<LookupIdxColumn> multiColumnJoinableLookupKeys = List.of(
            new LookupIdxColumn("id_int", "integer"),
            new LookupIdxColumn("name_str", "keyword"),
            new LookupIdxColumn("is_active_bool", "boolean"),
            new LookupIdxColumn("ip_addr", "ip"),
            new LookupIdxColumn("other1", "keyword"),
            new LookupIdxColumn("other2", "integer")
        );
        result.add(new LookupIdx("multi_column_joinable_lookup", multiColumnJoinableLookupKeys));
        return result;
    }

    List<CsvTestsDataLoader.EnrichConfig> availableEnrichPolicies() {
        return ENRICH_POLICIES;
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
                EsField editedField = new EsField(
                    field.getName(),
                    dataType,
                    field.getProperties(),
                    field.isAggregatable(),
                    field.getTimeSeriesFieldType()
                );
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
        if (preAnalysis.index() == null) {
            // If the data set doesn't matter we'll just grab one we know works. Employees is fine.
            return CsvTestsDataLoader.MultiIndexTestDataset.of(CSV_DATASET_MAP.get("employees"));
        }

        String indexName = preAnalysis.index().indexPattern();
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
            var testData = loadPageFromCsv(GenerativeCsvTests.class.getResource("/data/" + dataset.dataFileName()), dataset.typeMapping());
            Set<String> mappedFields = loadMapping(dataset.mappingFileName()).keySet();
            indexPages.add(new TestPhysicalOperationProviders.IndexPage(dataset.indexName(), testData.v1(), testData.v2(), mappedFields));
        }
        return TestPhysicalOperationProviders.create(foldCtx, indexPages);
    }

    @Override
    public QueryExecuted execute(String command, int depth) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        try {
            ActualResults result = executePlan(command, bigArrays);

            return new QueryExecuted(command, depth, toColumns(result.columnNames(), result.columnTypes()), data(result), null);
        } catch (Exception e) {
            return new QueryExecuted(command, depth, null, null, e);
        }
    }

    private List<Column> toColumns(List<String> names, List<Type> types) {
        List<Column> columns = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            columns.add(new Column(names.get(i), types.get(i).name(), List.of()));
        }
        return columns;
    }

    private List<List<Object>> data(ActualResults queryResult) {
        List<List<Object>> result = new ArrayList<>();
        if (queryResult.pages().isEmpty()) {
            return result;
        }
        int cols = queryResult.columnNames().size();
        for (Page page : queryResult.pages()) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>(cols);
                for (int c = 0; c < cols; c++) {
                    row.add(value(page.getBlock(c), p));
                }
                result.add(row);
            }
        }
        return result;
    }

    private Object value(Block block, int position) {
        BytesRef spare = new BytesRef();
        int valueCount = block.getValueCount(position);
        int idx = block.getFirstValueIndex(position);
        if (valueCount == 0) {
            return null;
        } else if (valueCount == 1) {
            return extract(block, idx, spare);
        } else {
            List<Object> values = new ArrayList<>(valueCount);
            for (int i = 0; i < valueCount; i++) {
                values.add(extract(block, idx + i, spare));
            }
            return values;
        }
    }

    private static Object extract(Block block, int idx, BytesRef spare) {
        return switch (block) {
            case ConstantNullBlock n -> null;
            case BooleanBlock b -> b.getBoolean(idx);
            case BytesRefBlock b -> b.getBytesRef(idx, spare);
            case IntBlock i -> i.getInt(idx);
            case LongBlock l -> l.getLong(idx);
            case FloatBlock f -> f.getFloat(idx);
            case DoubleBlock d -> d.getDouble(idx);
            default -> throw new IllegalArgumentException("Unsupported block type: " + block.getClass());
        };
    }

    private ActualResults executePlan(String query, BigArrays bigArrays) throws Exception {
        LogicalPlan parsed = parser.createStatement(query, EsqlTestUtils.TEST_CFG);
        var testDatasets = testDatasets(parsed);
        LogicalPlan analyzed = analyzedPlan(parsed, testDatasets);

        FoldContext foldCtx = FoldContext.small();
        EsqlSession session = new EsqlSession(
            getTestName(),
            configuration,
            null,
            null,
            null,
            new LogicalPlanPreOptimizer(new LogicalPreOptimizerContext(foldCtx)),
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

        session.preOptimizedPlan(analyzed, listener.delegateFailureAndWrap((l, preOptimized) -> {
            session.executeOptimizedPlan(
                new EsqlQueryRequest(),
                new EsqlExecutionInfo(randomBoolean()),
                planRunner(bigArrays, foldCtx, physicalOperationProviders),
                session.optimizedPlan(preOptimized),
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
        }));

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

    // Asserts that the serialization and deserialization of the plan creates an equivalent plan.
    private void opportunisticallyAssertPlanSerialization(PhysicalPlan plan) {
        if (plan.anyMatch(
            p -> p instanceof LocalSourceExec || p instanceof HashJoinExec || p instanceof ChangePointExec || p instanceof MergeExec
        )) {
            return;
        }
        SerializationTestUtils.assertSerialization(plan, configuration);
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
            mock(EnrichLookupService.class),
            mock(LookupFromIndexService.class),
            mock(InferenceService.class),
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
                new LocalPhysicalOptimizerContext(new EsqlFlags(true), configuration, foldCtx, searchStats)
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
