/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Listeners;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomMinimumVersion;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;

/** See GoldenTestsReadme.md for more information about these tests. */
@Listeners({ GoldenTestCase.GoldenTestReproduceInfoPrinter.class })
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // ExtrasFS can create extraneous files in the output directory.
public abstract class GoldenTestCase extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(GoldenTestCase.class);

    /**
     * RandomizedRunner appends {@code {seed=[...]}} to {@link #getTestName()} for {@code -Dtests.iters} / {@code @Repeat} (See #144763).
     */
    private static final Pattern RANDOMIZED_RUNNER_SEED_SUFFIX_AT_END = Pattern.compile("(?:\\s+\\{seed=\\[[^\\]]+\\]\\})+$");

    /**
     * Fixed sample probability that is used for all query approximation plans.
     * Normally it would be determined from subplan execution, but those are
     * not supported in the golden tests.
     */
    private static final double SAMPLE_PROBABILITY = 0.0123456789;

    private final Path baseFile;

    public GoldenTestCase() {
        try {
            String path = PathUtils.get(getClass().getResource(".").toURI()).toAbsolutePath().normalize().toString();
            var inSrc = path.replace('\\', '/').replaceFirst("build/classes/java/test", "src/test/resources");
            baseFile = PathUtils.get(Strings.format("%s/golden_tests/%s/", inSrc, getClass().getSimpleName()));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }

    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, String... nestedPath) {
        builder(esqlQuery).stages(stages).nestedPath(nestedPath).run();
    }

    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, SearchStats searchStats, String... nestedPath) {
        builder(esqlQuery).stages(stages).searchStats(searchStats).nestedPath(nestedPath).run();
    }

    protected void runGoldenTest(
        String esqlQuery,
        EnumSet<Stage> stages,
        SearchStats searchStats,
        TransportVersion transportVersion,
        String... nestedPath
    ) {
        String testName = RANDOMIZED_RUNNER_SEED_SUFFIX_AT_END.matcher(getTestName()).replaceFirst("");
        new Test(baseFile, testName, nestedPath, esqlQuery, stages, searchStats, transportVersion).doTest();
    }

    protected TestBuilder builder(String esqlQuery) {
        return new TestBuilder(esqlQuery);
    }

    protected final class TestBuilder {
        private final String esqlQuery;
        private EnumSet<Stage> stages;
        private SearchStats searchStats;
        private String[] nestedPath;
        private TransportVersion transportVersion;

        private TestBuilder(
            String esqlQuery,
            EnumSet<Stage> stages,
            SearchStats searchStats,
            String[] nestedPath,
            TransportVersion transportVersion
        ) {
            this.esqlQuery = esqlQuery;
            this.stages = stages;
            this.searchStats = searchStats;
            this.nestedPath = nestedPath;
            this.transportVersion = transportVersion;
        }

        TestBuilder(String esqlQuery) {
            this(esqlQuery, EnumSet.allOf(Stage.class), EsqlTestUtils.TEST_SEARCH_STATS, new String[0], randomMinimumVersion());
        }

        public TestBuilder stages(EnumSet<Stage> stages) {
            this.stages = stages;
            return this;
        }

        public EnumSet<Stage> stages() {
            return stages;
        }

        public TestBuilder searchStats(SearchStats searchStats) {
            this.searchStats = searchStats;
            return this;
        }

        public SearchStats searchStats() {
            return searchStats;
        }

        public TestBuilder nestedPath(String... nestedPath) {
            this.nestedPath = nestedPath;
            return this;
        }

        public String[] nestedPath() {
            return nestedPath;
        }

        public TransportVersion transportVersion() {
            return transportVersion;
        }

        public TestBuilder transportVersion(TransportVersion transportVersion) {
            this.transportVersion = transportVersion;
            return this;
        }

        public void run() {
            runGoldenTest(esqlQuery, stages, searchStats, transportVersion, nestedPath);
        }

        public Optional<Throwable> tryRun() {
            try {
                run();
                return Optional.empty();
            } catch (Throwable e) {
                return Optional.of(e);
            }
        }
    }

    private record Test(
        Path basePath,
        String testName,
        String[] nestedPath,
        String esqlQuery,
        EnumSet<Stage> stages,
        SearchStats searchStats,
        TransportVersion transportVersion
    ) {

        private void doTest() {
            try {
                var results = doTests();
                var failedStages = results.stream().filter(e -> e.v2() == TestResult.FAILURE).map(Tuple::v1).toList();
                if (failedStages.isEmpty() == false) {
                    fail(Strings.format("Output for test '%s' does not match for stages '%s'", testName, failedStages));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private List<Tuple<Stage, TestResult>> doTests() throws IOException {
            EsqlStatement statement = TEST_PARSER.createStatement(esqlQuery);
            LogicalPlan parsedPlan = statement.plan();
            String[] queryPathParts = new String[nestedPath.length + 2];
            queryPathParts[0] = testName;
            System.arraycopy(nestedPath, 0, queryPathParts, 1, nestedPath.length);
            queryPathParts[queryPathParts.length - 1] = "query.esql";
            Path queryPath = PathUtils.get(basePath.toString(), queryPathParts);
            Files.createDirectories(queryPath.getParent());
            Files.writeString(queryPath, esqlQuery);
            UnmappedResolution unmappedResolution = statement.setting(UNMAPPED_FIELDS);
            TestAnalyzer testAnalyzer = analyzer().addLanguagesLookup()
                .addTestLookup()
                .addAnalysisTestsEnrichResolution()
                .addAnalysisTestsInferenceResolution()
                .minimumTransportVersion(transportVersion)
                .unmappedResolution(unmappedResolution);
            boolean trackUnmappedFieldIndices = unmappedResolution == UnmappedResolution.LOAD
                || parsedPlan.anyMatch(p -> p instanceof Insist);
            loadIndexResolution(testDatasets(parsedPlan), trackUnmappedFieldIndices).forEach(
                (pattern, resolution) -> testAnalyzer.addIndex(pattern.indexPattern(), resolution)
            );
            Analyzer analyzer = testAnalyzer.buildAnalyzer();
            List<Tuple<Stage, TestResult>> result = new ArrayList<>();
            var analyzed = analyzer.analyze(parsedPlan);
            if (stages.contains(Stage.ANALYSIS)) {
                result.add(Tuple.tuple(Stage.ANALYSIS, verifyOrWrite(analyzed, Stage.ANALYSIS)));
            }
            if (stages.equals(EnumSet.of(Stage.ANALYSIS))) {
                return result;
            }
            var configuration = EsqlTestUtils.configuration(new QueryPragmas(Settings.EMPTY), esqlQuery, statement);
            var optimizerContext = new LogicalOptimizerContext(configuration, FoldContext.small(), transportVersion);
            var logicallyOptimized = new LogicalPlanOptimizer(optimizerContext).optimize(analyzed);
            if (stages.contains(Stage.LOGICAL_OPTIMIZATION)) {
                result.add(Tuple.tuple(Stage.LOGICAL_OPTIMIZATION, verifyOrWrite(logicallyOptimized, Stage.LOGICAL_OPTIMIZATION)));
            }
            if (stages.contains(Stage.PHYSICAL_OPTIMIZATION)
                || stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)
                || stages.contains(Stage.LOOKUP_LOGICAL_OPTIMIZATION)
                || stages.contains(Stage.LOOKUP_PHYSICAL_OPTIMIZATION)
                || stages.contains(Stage.NODE_REDUCE)
                || stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                // When query approximation is enabled, the logical plan can contain
                // `SampleProbabilityPlaceholder`s. After subplan execution, these are replaced
                // by literal sample probabilities. This is required for physical plan
                // optimization. Since subplan execution is not done in the golden tests,
                // manually replace the placeholders instead by a fixed value.
                logicallyOptimized = ApproximationPlan.substituteSampleProbability(logicallyOptimized, SAMPLE_PROBABILITY);
                var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, transportVersion));
                PhysicalPlan physicalPlan = physicalPlanOptimizer.optimize(
                    new Mapper().map(new Versioned<>(logicallyOptimized, transportVersion))
                );
                if (stages.contains(Stage.PHYSICAL_OPTIMIZATION)) {
                    result.add(Tuple.tuple(Stage.PHYSICAL_OPTIMIZATION, verifyOrWrite(physicalPlan, Stage.PHYSICAL_OPTIMIZATION)));
                }
                PhysicalPlan localPhysicalPlan = null;
                boolean needsLocalPlan = stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)
                    || stages.contains(Stage.LOOKUP_LOGICAL_OPTIMIZATION)
                    || stages.contains(Stage.LOOKUP_PHYSICAL_OPTIMIZATION);
                if (needsLocalPlan) {
                    localPhysicalPlan = localOptimize(physicalPlan, configuration);
                }
                if (stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)) {
                    TestResult localPhysicalResult = verifyOrWrite(localPhysicalPlan, Stage.LOCAL_PHYSICAL_OPTIMIZATION);
                    result.add(Tuple.tuple(Stage.LOCAL_PHYSICAL_OPTIMIZATION, localPhysicalResult));
                }
                if (stages.contains(Stage.LOOKUP_LOGICAL_OPTIMIZATION) || stages.contains(Stage.LOOKUP_PHYSICAL_OPTIMIZATION)) {
                    List<LookupJoinExec> joins = findLookupJoins(localPhysicalPlan);
                    for (int i = 0; i < joins.size(); i++) {
                        String suffix = joins.size() > 1 ? "_" + i : "";
                        LogicalPlan lookupLogical = buildLookupLogicalPlan(joins.get(i), configuration, searchStats);
                        if (stages.contains(Stage.LOOKUP_LOGICAL_OPTIMIZATION)) {
                            TestResult r = verifyOrWrite(lookupLogical, outputPath("lookup_logical_optimization" + suffix));
                            result.add(Tuple.tuple(Stage.LOOKUP_LOGICAL_OPTIMIZATION, r));
                        }
                        if (stages.contains(Stage.LOOKUP_PHYSICAL_OPTIMIZATION)) {
                            PhysicalPlan lookupPhysical = optimizeLookupPhysicalPlan(lookupLogical, configuration, searchStats);
                            TestResult r = verifyOrWrite(lookupPhysical, outputPath("lookup_physical_optimization" + suffix));
                            result.add(Tuple.tuple(Stage.LOOKUP_PHYSICAL_OPTIMIZATION, r));
                        }
                    }
                }
                if (stages.contains(Stage.NODE_REDUCE) || stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                    ExchangeExec exec = EsqlTestUtils.singleValue(physicalPlan.collect(ExchangeExec.class));
                    var sink = new ExchangeSinkExec(exec.source(), exec.output(), false, exec.child());
                    var reductionPlan = ComputeService.reductionPlan(
                        PlannerSettings.DEFAULTS,
                        new EsqlFlags(false),
                        configuration,
                        configuration.newFoldContext(),
                        sink,
                        true,
                        true,
                        new PlanTimeProfile()

                    );
                    if (stages.contains(Stage.NODE_REDUCE)) {
                        var dualFileOutput = (DualFileOutput) Stage.NODE_REDUCE.fileOutput;
                        result.addAll(
                            addNodeReduceDualPlanResult(reductionPlan, dualFileOutput.nodeReduceOutput(), dualFileOutput.dataNodeOutput())
                        );
                    }
                    if (stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                        var singleFileOutput = (SingleFileOutput) Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION.fileOutput;
                        result.add(
                            Tuple.tuple(
                                Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION,
                                verifyOrWrite(
                                    localOptimize(reductionPlan.dataNodePlan(), configuration),
                                    outputPath(singleFileOutput.output())
                                )
                            )
                        );
                    }
                }
            }
            return result;
        }

        private enum TestResult {
            SUCCESS,
            FAILURE,
            CREATED
        }

        private List<Tuple<Stage, TestResult>> addNodeReduceDualPlanResult(ReductionPlan plan, String nodeReduceName, String dataNodeName)
            throws IOException {
            var stage = Stage.NODE_REDUCE;
            var reduceResult = verifyOrWrite(plan.nodeReducePlan(), outputPath(nodeReduceName));
            var dataResult = verifyOrWrite(plan.dataNodePlan(), outputPath(dataNodeName));
            var result = new ArrayList<Tuple<Stage, TestResult>>();
            if (reduceResult == TestResult.FAILURE || dataResult == TestResult.FAILURE) {
                result.add(Tuple.tuple(stage, TestResult.FAILURE));
            } else if (reduceResult == TestResult.CREATED || dataResult == TestResult.CREATED) {
                if (reduceResult != dataResult) {
                    throw new IllegalStateException("Both local reduction and local data plan should be created for a new test");
                }
                result.add(Tuple.tuple(stage, TestResult.CREATED));
            } else {
                if (reduceResult != TestResult.SUCCESS || dataResult != TestResult.SUCCESS) {
                    throw new IllegalStateException("Both local reduction and local data plan should be successful at this point");
                }
                result.add(Tuple.tuple(stage, TestResult.SUCCESS));
            }
            return result;
        }

        private <T extends QueryPlan<T>> TestResult verifyOrWrite(T plan, Stage stage) throws IOException {
            return verifyOrWrite(plan, outputPath(stage));
        }

        private <T extends QueryPlan<T>> TestResult verifyOrWrite(T plan, Path outputFile) throws IOException {
            if (System.getProperty("golden.overwrite") != null) {
                logger.info("Overwriting file {}", outputFile);
                return createNewOutput(outputFile, plan);
            } else {
                if (Files.exists(outputFile)) {
                    return verifyExisting(outputFile, plan);
                } else {
                    logger.debug("No output exists for file {}, writing new output", outputFile);
                    return createNewOutput(outputFile, plan);
                }
            }
        }

        private Path outputPath(Stage stage) {
            return outputPath(((SingleFileOutput) stage.fileOutput).output());
        }

        private Path outputPath(String stageName) {
            var paths = new String[nestedPath.length + 2];
            paths[0] = testName;
            System.arraycopy(nestedPath, 0, paths, 1, nestedPath.length);
            paths[paths.length - 1] = Strings.format("%s.expected", stageName);
            return PathUtils.get(basePath.toString(), paths);
        }

        private PhysicalPlan localOptimize(PhysicalPlan plan, Configuration conf) {
            return PlannerUtils.localPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                conf,
                conf.newFoldContext(),
                plan,
                searchStats,
                new PlanTimeProfile()
            );
        }

        private static List<LookupJoinExec> findLookupJoins(PhysicalPlan plan) {
            List<LookupJoinExec> joins = new ArrayList<>();
            plan.forEachDown(p -> {
                if (p instanceof LookupJoinExec join) {
                    joins.add(join);
                }
            });
            return joins;
        }

        private static LogicalPlan buildLookupLogicalPlan(LookupJoinExec join, Configuration conf, SearchStats stats) {
            List<MatchConfig> matchFields = new ArrayList<>(join.leftFields().size());
            for (int i = 0; i < join.leftFields().size(); i++) {
                FieldAttribute right = (FieldAttribute) join.rightFields().get(i);
                String fieldName = right.exactAttribute().fieldName().string();
                if (join.isOnJoinExpression()) {
                    fieldName = join.leftFields().get(i).name();
                }
                matchFields.add(new MatchConfig(fieldName, i, join.leftFields().get(i).dataType()));
            }
            LogicalPlan logicalPlan = LookupFromIndexService.buildLocalLogicalPlan(
                join.source(),
                matchFields,
                join.joinOnConditions(),
                join.right(),
                join.addedFields().stream().map(f -> (NamedExpression) f).toList()
            );
            FoldContext foldCtx = conf.newFoldContext();
            return new LookupLogicalOptimizer(new LocalLogicalOptimizerContext(conf, foldCtx, stats)).localOptimize(logicalPlan);
        }

        private static PhysicalPlan optimizeLookupPhysicalPlan(LogicalPlan logicalPlan, Configuration conf, SearchStats stats) {
            FoldContext foldCtx = conf.newFoldContext();
            PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(logicalPlan);
            var context = new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, new EsqlFlags(true), conf, foldCtx, stats);
            return new LookupPhysicalPlanOptimizer(context).optimize(physicalPlan);
        }
    }

    private static Test.TestResult createNewOutput(Path output, QueryPlan<?> plan) throws IOException {
        if (output.toString().contains("extra")) {
            throw new IllegalStateException("Extra output files should not be created automatically:" + output);
        }
        String full = plan.toString(Node.NodeStringFormat.FULL);
        Files.writeString(output, normalizeString(full), StandardCharsets.UTF_8);
        return Test.TestResult.CREATED;
    }

    // Visible for testing.
    static String normalizeString(String input) {
        return normalizeRandomSeeds(normalizeNameIds(normalizeSyntheticNames(input)));
    }

    /**
     * Rewrites seeds of random sampling queries to a fixed seed of 42.
     * The seed generated during plan building can vary between runs, so this is needed to keep golden output deterministic.
     */
    private static String normalizeRandomSeeds(String line) {
        return line.replaceAll("(\"seed\"\\s*:\\s*)(-?\\d+)", "$142");
    }

    /**
     * Rewrites node IDs ({@code #n}) in the plan string to a stable numbering by order of first appearance.
     * Actual IDs assigned during plan building can vary between runs, so this is needed to keep golden output deterministic.
     */
    private static String normalizeNameIds(String planString) {
        return replaceMatches(planString, IDENTIFIER_PATTERN, (matcher, idMap) -> {
            int originalId = Integer.parseInt(matcher.group().substring(1)); // Drop the initial '#' prefix
            return "#" + idMap.getId(originalId);
        });
    }

    /**
     * Normalizes synthetic attribute names of the form $$something($something)* that are followed by # (node id).
     * Each distinct synthetic name is assigned a stable id by order of first appearance in the plan, and that id
     * replaces every digit-only segment in the name when rebuilt; text segments are kept as-is. Digits may appear
     * anywhere in the name, including in the middle (e.g. {@code $$SUM$field$0$sum}).
     * <p>
     * Keying by the full name (rather than just the digit segments) ensures that two unrelated synthetic names
     * with different text prefixes get independent ids, even when their digit tails happen to collide because
     * the JVM-global counters that produced them drifted differently across test runs.
     */
    private static String normalizeSyntheticNames(String full) {
        return replaceMatches(full, SYNTHETIC_PATTERN, (matcher, idMap) -> {
            StringBuilder result = new StringBuilder("$$");
            StringBuilder numericSegments = new StringBuilder();
            boolean hasNormalized = false;
            for (String seg : matcher.group(1).split("\\$")) {
                if (NUMERIC_SEGMENT_PATTERN.matcher(seg).matches()) {
                    appendSegment(numericSegments, seg);
                } else {
                    if (numericSegments.isEmpty() == false) {
                        appendSegment(result, idMap.getId(matcher.group(1)));
                        numericSegments.setLength(0);
                        hasNormalized = true;
                    }
                    appendSegment(result, seg);
                }
            }
            if (numericSegments.isEmpty() == false) {
                appendSegment(result, idMap.getId(matcher.group(1)));
                hasNormalized = true;
            }
            return hasNormalized ? result.toString() : matcher.group();
        });
    }

    private static StringBuilder appendSegment(StringBuilder sb, Object o) {
        if (sb.isEmpty() || sb.charAt(sb.length() - 1) != '$') {
            sb.append('$');
        }
        return sb.append(o);
    }

    private static <K> String replaceMatches(String input, Pattern pattern, BiFunction<Matcher, IdMap<K>, String> replacer) {
        var idMap = new IdMap<K>();
        Matcher matcher = pattern.matcher(input);
        StringBuilder sb = new StringBuilder();
        int lastEnd = 0;
        while (matcher.find()) {
            sb.append(input, lastEnd, matcher.start());
            sb.append(replacer.apply(matcher, idMap));
            lastEnd = matcher.end();
        }
        sb.append(input, lastEnd, input.length());
        return sb.toString();
    }

    /**
     * Matches synthetic names like {@code $$alias$1$2#3}, {@code $$last_name$LENGTH$241149320{f$}#6}, or
     * {@code $$SUM$field$0$sum#7}. Digit-only segments are generated during the test run and may differ
     * each time; text segments are kept. The {@code #digit} suffixes are normalized by
     * {@link #IDENTIFIER_PATTERN}.
     */
    private static final Pattern SYNTHETIC_PATTERN = Pattern.compile("\\$\\$([^$\\s{#]+(?:\\$[^$\\s{#]+)*)(?=[{#])");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("#\\d+");
    private static final Pattern NUMERIC_SEGMENT_PATTERN = Pattern.compile("-?\\d+");

    private static class IdMap<K> {
        private final Map<K, Integer> map = new HashMap<>();
        private int counter = 0;

        public int getId(K key) {
            return map.computeIfAbsent(key, k -> counter++);
        }
    }

    private static Test.TestResult verifyExisting(Path output, QueryPlan<?> plan) throws IOException {
        String full = plan.toString(Node.NodeStringFormat.FULL);
        String actualString = normalize(normalizeString(full));
        String expectedString = normalize(Files.readString(output));
        if (actualString.equals(expectedString)) {
            if (System.getProperty("golden.cleanactual") != null) {
                Path path = actualPath(output);
                if (Files.exists(path)) {
                    logger.debug(Strings.format("Cleaning up actual file '%s' because golden.cleanactual property is set", path));
                    Files.delete(path);
                }
            }
            return Test.TestResult.SUCCESS;
        }

        if (System.getProperty("golden.noactual") != null) {
            logger.debug("Skipping actual file creation because golden.noactual property is set");
        } else {
            Path actualPath = actualPath(output);
            logger.info("Creating actual file at " + actualPath.toAbsolutePath());
            Files.writeString(actualPath, normalizeString(full), StandardCharsets.UTF_8);
        }

        logger.info("Test failure:\n[Actual]\n{}\n[Expected]\n{}\n", actualString, expectedString);
        return Test.TestResult.FAILURE;
    }

    private static Path actualPath(Path output) {
        return output.resolveSibling(output.getFileName().toString().replaceAll("expected", "actual"));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private sealed interface StageOutput {}

    private record SingleFileOutput(String output) implements StageOutput {}

    private record DualFileOutput(String nodeReduceOutput, String dataNodeOutput) implements StageOutput {}

    protected enum Stage {
        /** See {@link Analyzer}. */
        ANALYSIS(new SingleFileOutput("analysis")),
        /** See {@link LogicalPlanOptimizer}. */
        LOGICAL_OPTIMIZATION(new SingleFileOutput("logical_optimization")),
        /** See {@link PhysicalPlanOptimizer}. */
        PHYSICAL_OPTIMIZATION(new SingleFileOutput("physical_optimization")),
        /**
         * See {@link LocalPhysicalPlanOptimizer}. There's no LOCAL_LOGICAL here since in production we use PlannerUtils.localPlan to
         * produce the local physical plan directly from non-local physical plan.
         */
        LOCAL_PHYSICAL_OPTIMIZATION(new SingleFileOutput("local_physical_optimization")),
        /**
         * Lookup-node logical optimization: builds a logical plan from each {@link LookupJoinExec} and runs
         * {@link LookupLogicalOptimizer}. When a query contains multiple LOOKUP JOINs, each produces a separate output file.
         */
        LOOKUP_LOGICAL_OPTIMIZATION(new SingleFileOutput("lookup_logical_optimization")),
        /**
         * Lookup-node physical optimization: runs {@link LookupPhysicalPlanOptimizer} on each lookup plan.
         * When a query contains multiple LOOKUP JOINs, each produces a separate output file.
         */
        LOOKUP_PHYSICAL_OPTIMIZATION(new SingleFileOutput("lookup_physical_optimization")),
        /**
         * See {@link ComputeService#reductionPlan}. Actually results in <b>two</b> plans: one for the node reduce driver and one for the
         * data nodes.
         */
        NODE_REDUCE(new DualFileOutput("local_reduce_planned_reduce_driver", "local_reduce_planned_data_driver")),

        /**
         * A {@link Stage#LOCAL_PHYSICAL_OPTIMIZATION} performed on the data node plan after splitting off the node reduce plan. Since
         * the node-reduce plan isn't optimized after being created, there is only one output to test here.
         */
        NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION(new SingleFileOutput("local_reduce_physical_optimization_data_driver"));

        private final StageOutput fileOutput;

        Stage(StageOutput fileOutput) {
            this.fileOutput = fileOutput;
        }
    }

    private static String normalize(String s) {
        return s.lines().map(String::strip).collect(Collectors.joining("\n"));
    }

    /**
     * Adds -Dgolden.overwrite to the reproduction line for golden test failures. This has to be a nested class to get pass the
     * {@code TestingConventionsCheckWorkAction} check, which incorrectly identifies this class as a test.
     */
    public static class GoldenTestReproduceInfoPrinter extends RunListener {
        private final ReproduceInfoPrinter delegate = new ReproduceInfoPrinter();

        @Override
        public void testFailure(Failure failure) throws Exception {
            if (failure.getException() instanceof AssumptionViolatedException) {
                return;
            }
            if (isGoldenTest(failure)) {
                printToErr(captureDelegate(failure).replace("REPRODUCE WITH:", "OVERWRITE WITH:") + " -Dgolden.overwrite");
            } else {
                delegate.testFailure(failure);
            }
        }

        @SuppressForbidden(reason = "Using System.err to redirect output")
        private String captureDelegate(Failure failure) throws Exception {
            PrintStream originalErr = System.err;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setErr(new PrintStream(baos, true, StandardCharsets.UTF_8));
            try {
                delegate.testFailure(failure);
            } finally {
                System.setErr(originalErr);
            }
            return baos.toString(StandardCharsets.UTF_8).trim();
        }

        private static boolean isGoldenTest(Failure failure) {
            try {
                return GoldenTestCase.class.isAssignableFrom(Class.forName(failure.getDescription().getClassName()));
            } catch (ClassNotFoundException e) {
                return false;
            }
        }

        @SuppressForbidden(reason = "printing repro info")
        private static void printToErr(String s) {
            System.err.println(s);
        }
    }

    private static Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> testDatasets(LogicalPlan parsed) {
        var preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        if (preAnalysis.indexes().isEmpty()) {
            // If the data set doesn't matter we'll just grab one we know works. Employees is fine.
            return Map.of(
                new IndexPattern(Source.EMPTY, "employees"),
                CsvTestsDataLoader.MultiIndexTestDataset.of(CSV_DATASET.get("employees"))
            );
        }

        List<String> missing = new ArrayList<>();
        Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> all = new HashMap<>();
        for (IndexPattern indexPattern : preAnalysis.indexes().keySet()) {
            List<CsvTestsDataLoader.TestDataset> datasets = new ArrayList<>();
            String indexName = indexPattern.indexPattern();
            if (indexName.endsWith("*")) {
                String indexPrefix = indexName.substring(0, indexName.length() - 1);
                for (var entry : CSV_DATASET.entrySet()) {
                    if (entry.getKey().startsWith(indexPrefix)) {
                        datasets.add(entry.getValue());
                    }
                }
            } else {
                for (String index : indexName.split(",")) {
                    var dataset = CSV_DATASET.get(index);
                    if (dataset == null) {
                        throw new IllegalArgumentException("unknown CSV dataset for table [" + index + "]");
                    }
                    datasets.add(dataset);
                }
            }
            if (datasets.isEmpty() == false) {
                all.put(indexPattern, new CsvTestsDataLoader.MultiIndexTestDataset(indexName, datasets));
            } else {
                missing.add(indexName);
            }
        }
        if (all.isEmpty()) {
            throw new IllegalArgumentException("Found no CSV datasets for table [" + preAnalysis.indexes() + "]");
        }
        if (missing.isEmpty() == false) {
            throw new IllegalArgumentException("Did not find datasets for tables: " + missing);
        }
        return all;
    }

    public static Map<IndexPattern, IndexResolution> loadIndexResolution(
        Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> datasets,
        boolean trackUnmappedFieldIndices
    ) {
        Map<IndexPattern, IndexResolution> indexResolutions = new HashMap<>();
        for (var entry : datasets.entrySet()) {
            indexResolutions.put(entry.getKey(), loadIndexResolution(entry.getValue(), trackUnmappedFieldIndices));
        }
        return indexResolutions;
    }

    public static Map<IndexPattern, IndexResolution> loadIndexResolution(
        Map<IndexPattern, CsvTestsDataLoader.MultiIndexTestDataset> datasets
    ) {
        return loadIndexResolution(datasets, false);
    }

    public static IndexResolution loadIndexResolution(CsvTestsDataLoader.MultiIndexTestDataset datasets) {
        return loadIndexResolution(datasets, false);
    }

    public static IndexResolution loadIndexResolution(
        CsvTestsDataLoader.MultiIndexTestDataset datasets,
        boolean trackUnmappedFieldIndices
    ) {
        var indexNames = datasets.datasets().stream().map(CsvTestsDataLoader.TestDataset::indexName);
        Map<String, IndexMode> indexModes = indexNames.collect(Collectors.toMap(x -> x, x -> IndexMode.STANDARD));
        List<MappingPerIndex> mappings = datasets.datasets()
            .stream()
            .map(ds -> new MappingPerIndex(ds.indexName(), createMappingForIndex(ds)))
            .toList();
        var mergedMappings = mergeMappings(mappings, trackUnmappedFieldIndices);
        return IndexResolution.valid(new EsIndex(datasets.indexPattern(), mergedMappings.mapping, indexModes, Map.of(), Map.of()));
    }

    // TODO should de-duplicate, strong overlap with CsvTestsDataLoader#readMappingFile
    private static Map<String, EsField> createMappingForIndex(CsvTestsDataLoader.TestDataset dataset) {
        var mapping = new TreeMap<>(LoadMapping.loadMapping(dataset.streamMapping()));
        if (dataset.typeMapping() != null) {
            for (var entry : dataset.typeMapping().entrySet()) {
                String key = entry.getKey();
                String[] segments = key.split("\\.");
                // Navigate to the parent map containing the leaf field.
                Map<String, EsField> targetMap = mapping;
                for (int i = 0; i < segments.length - 1 && targetMap != null; i++) {
                    EsField parent = targetMap.get(segments[i]);
                    targetMap = parent != null ? parent.getProperties() : null;
                }
                String leafName = segments[segments.length - 1];
                if (targetMap == null) {
                    continue;
                }

                if (entry.getValue() == null) {
                    targetMap.remove(leafName);
                    continue;
                }
                if (targetMap.containsKey(leafName)) {
                    DataType dataType = DataType.fromTypeName(entry.getValue());
                    EsField field = targetMap.get(leafName);
                    EsField editedField = new EsField(
                        field.getName(),
                        dataType,
                        field.getProperties(),
                        field.isAggregatable(),
                        field.getTimeSeriesFieldType()
                    );
                    targetMap.put(leafName, editedField);
                }
            }
        }
        // Add dynamic mappings, but only if they are not already mapped
        if (dataset.dynamicTypeMapping() != null) {
            for (var entry : dataset.dynamicTypeMapping().entrySet()) {
                if (mapping.containsKey(entry.getKey()) == false) {
                    DataType dataType = DataType.fromTypeName(entry.getValue());
                    EsField editedField = new EsField(entry.getKey(), dataType, Map.of(), false, EsField.TimeSeriesFieldType.NONE);
                    mapping.put(entry.getKey(), editedField);
                }
            }
        }
        return mapping;
    }

    record MappingPerIndex(String index, Map<String, EsField> mapping) {}

    record MergedResult(Map<String, EsField> mapping) {}

    private static MergedResult mergeMappings(List<MappingPerIndex> mappingsPerIndex, boolean trackUnmappedFieldIndices) {
        Map<String, Map<String, EsField>> fieldNamesToFieldByIndices = new HashMap<>();
        for (var mappingPerIndex : mappingsPerIndex) {
            for (var entry : mappingPerIndex.mapping().entrySet()) {
                fieldNamesToFieldByIndices.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                    .put(mappingPerIndex.index(), entry.getValue());
            }
        }
        int numberOfIndices = mappingsPerIndex.size();
        var mappings = fieldNamesToFieldByIndices.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
            String fieldName = e.getKey();
            Map<String, EsField> indexToFields = e.getValue();
            EsField field = mergeFields(fieldName, indexToFields);
            return trackUnmappedFieldIndices
                ? IndexResolver.wrapIfPartiallyUnmapped(field, fieldName, fieldName, indexToFields.keySet(), numberOfIndices)
                : field;
        }));
        return new MergedResult(mappings);
    }

    private static EsField mergeFields(String fieldName, Map<String, EsField> indexToFields) {
        var fields = indexToFields.values();
        if (fields.stream().distinct().count() > 1) {
            var typesToIndices = new HashMap<String, Set<String>>();
            for (var indexToField : indexToFields.entrySet()) {
                typesToIndices.computeIfAbsent(indexToField.getValue().getDataType().typeName(), k -> new HashSet<>())
                    .add(indexToField.getKey());
            }
            return new InvalidMappedField(fieldName, typesToIndices);
        } else {
            return fields.iterator().next();
        }
    }
}
