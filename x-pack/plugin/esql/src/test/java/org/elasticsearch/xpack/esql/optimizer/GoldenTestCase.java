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
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.xpack.esql.CsvTests;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.ReductionPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
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
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomMinimumVersion;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;

/** See GoldenTestsReadme.md for more information about these tests. */
@Listeners({ GoldenTestCase.GoldenTestReproduceInfoPrinter.class })
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // ExtrasFS can create extraneous files in the output directory.
public abstract class GoldenTestCase extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(GoldenTestCase.class);
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
        String testName = extractTestName();
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
            LogicalPlan parsedStatement = EsqlParser.INSTANCE.parseQuery(esqlQuery);
            Files.createDirectories(PathUtils.get(basePath.toString(), testName));
            Files.writeString(PathUtils.get(basePath.toString(), testName, "query.esql"), esqlQuery);
            var analyzer = new Analyzer(
                new AnalyzerContext(
                    EsqlTestUtils.TEST_CFG,
                    new EsqlFunctionRegistry(),
                    CsvTests.loadIndexResolution(CsvTests.testDatasets(parsedStatement)),
                    defaultLookupResolution(),
                    new EnrichResolution(),
                    InferenceResolution.EMPTY,
                    transportVersion,
                    UnmappedResolution.FAIL
                ),
                TEST_VERIFIER
            );
            List<Tuple<Stage, TestResult>> result = new ArrayList<>();
            var analyzed = analyzer.analyze(parsedStatement);
            if (stages.contains(Stage.ANALYSIS)) {
                result.add(Tuple.tuple(Stage.ANALYSIS, verifyOrWrite(analyzed, Stage.ANALYSIS)));
            }
            if (stages.equals(EnumSet.of(Stage.ANALYSIS))) {
                return result;
            }
            var optimizerContext = new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), transportVersion);
            var logicallyOptimized = new LogicalPlanOptimizer(optimizerContext).optimize(analyzed);
            if (stages.contains(Stage.LOGICAL_OPTIMIZATION)) {
                result.add(Tuple.tuple(Stage.LOGICAL_OPTIMIZATION, verifyOrWrite(logicallyOptimized, Stage.LOGICAL_OPTIMIZATION)));
            }
            if (stages.contains(Stage.PHYSICAL_OPTIMIZATION)
                || stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)
                || stages.contains(Stage.NODE_REDUCE)
                || stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                var physicalPlanOptimizer = new PhysicalPlanOptimizer(
                    new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, transportVersion)
                );
                PhysicalPlan physicalPlan = physicalPlanOptimizer.optimize(
                    new Mapper().map(new Versioned<>(logicallyOptimized, transportVersion))
                );
                if (stages.contains(Stage.PHYSICAL_OPTIMIZATION)) {
                    result.add(Tuple.tuple(Stage.PHYSICAL_OPTIMIZATION, verifyOrWrite(physicalPlan, Stage.PHYSICAL_OPTIMIZATION)));
                }
                Configuration conf = analyzer.context().configuration();
                if (stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)) {
                    TestResult localPhysicalResult = verifyOrWrite(localOptimize(physicalPlan, conf), Stage.LOCAL_PHYSICAL_OPTIMIZATION);
                    result.add(Tuple.tuple(Stage.LOCAL_PHYSICAL_OPTIMIZATION, localPhysicalResult));
                }
                if (stages.contains(Stage.NODE_REDUCE) || stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                    ExchangeExec exec = EsqlTestUtils.singleValue(physicalPlan.collect(ExchangeExec.class));
                    var sink = new ExchangeSinkExec(exec.source(), exec.output(), false, exec.child());
                    var reductionPlan = ComputeService.reductionPlan(
                        PlannerSettings.DEFAULTS,
                        new EsqlFlags(false),
                        conf,
                        conf.newFoldContext(),
                        sink,
                        true,
                        true,
                        new PlanTimeProfile()

                    );
                    if (stages.contains(Stage.NODE_REDUCE)) {
                        var dualFileOutput = (DualFileOutput) Stage.NODE_REDUCE.fileOutput;
                        result.addAll(
                            addDualPlanResult(
                                Stage.NODE_REDUCE,
                                reductionPlan,
                                dualFileOutput.nodeReduceOutput(),
                                dualFileOutput.dataNodeOutput()
                            )
                        );
                    }
                    if (stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                        var dualFileOutput = (DualFileOutput) Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION.fileOutput;
                        switch (reductionPlan.localPhysicalOptimization()) {
                            // If there is no local node-reduce physical optimization, there's nothing to verify!
                            case DISABLED -> {
                                result.add(
                                    Tuple.tuple(
                                        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION,
                                        verifyOrWrite(
                                            localOptimize(reductionPlan.dataNodePlan(), conf),
                                            outputPath(dualFileOutput.dataNodeOutput())
                                        )
                                    )
                                );
                            }
                            case ENABLED -> {
                                var finalizedResult = new ReductionPlan(
                                    (ExchangeSinkExec) localOptimize(reductionPlan.nodeReducePlan(), conf),
                                    (ExchangeSinkExec) localOptimize(reductionPlan.dataNodePlan(), conf),
                                    reductionPlan.localPhysicalOptimization()
                                );
                                result.addAll(
                                    addDualPlanResult(
                                        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION,
                                        finalizedResult,
                                        dualFileOutput.nodeReduceOutput(),
                                        dualFileOutput.dataNodeOutput()
                                    )
                                );
                            }
                        }
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

        private List<Tuple<Stage, TestResult>> addDualPlanResult(
            Stage stage,
            ReductionPlan plan,
            String nodeReduceName,
            String dataNodeName
        ) throws IOException {
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
    }

    private static Test.TestResult createNewOutput(Path output, QueryPlan<?> plan) throws IOException {
        if (output.toString().contains("extra")) {
            throw new IllegalStateException("Extra output files should not be created automatically:" + output);
        }
        Files.writeString(output, toString(plan), StandardCharsets.UTF_8);
        return Test.TestResult.CREATED;
    }

    private static String toString(Node<?> plan) {
        String planString = plan.toString(Node.NodeStringFormat.FULL);
        String withoutSyntheticPatterns = SYNTHETIC_PATTERN.matcher(planString).replaceAll("\\$\\$$1");
        return IDENTIFIER_PATTERN.matcher(withoutSyntheticPatterns).replaceAll("");
    }

    // Matches synthetic names like $$alias$1$2#3, since those $digits are generated during the test run and may differ each time. The
    // #digit are removed by the next pattern.
    private static final Pattern SYNTHETIC_PATTERN = Pattern.compile("\\$\\$([^$\\s]+)(\\$\\d+)*(?=[{#])");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("#\\d+");

    private static Test.TestResult verifyExisting(Path output, QueryPlan<?> plan) throws IOException {
        String testString = normalize(toString(plan));
        if (testString.equals(normalize(Files.readString(output)))) {
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
            List<String> actualLines = testString.lines().map(GoldenTestCase::normalize).toList();
            Path actualPath = actualPath(output);
            logger.info("Creating actual file at " + actualPath.toAbsolutePath());
            Files.write(actualPath, actualLines);
        }
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
         * See {@link ComputeService#reductionPlan}. Actually results in <b>two</b> plans: one for the node reduce driver and one for the
         * data nodes.
         */
        NODE_REDUCE(new DualFileOutput("local_reduce_planned_reduce_driver", "local_reduce_planned_data_driver")),
        /**
         * A combination of {@link Stage#NODE_REDUCE} and {@link  Stage#LOCAL_PHYSICAL_OPTIMIZATION}: first produce the node
         * reduce and data node plans, and then perform local physical optimization on both.
         */
        // TODO should result in only one plan, see https://github.com/elastic/elasticsearch/issues/142392.
        NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION(
            new DualFileOutput("local_reduce_physical_optimization_reduce_driver", "local_reduce_physical_optimization_data_driver")
        );

        private final StageOutput fileOutput;

        Stage(StageOutput fileOutput) {
            this.fileOutput = fileOutput;
        }
    }

    private sealed interface TestOutput {}

    private record SingleTestOutput(String output) implements TestOutput {}

    private record DualTestOutput(String nodeReduceOutput, String dataNodeOutput) implements TestOutput {}

    private static String normalize(String s) {
        return s.lines().map(String::strip).collect(Collectors.joining("\n"));
    }

    private String extractTestName() {
        return getTestName();
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
}
