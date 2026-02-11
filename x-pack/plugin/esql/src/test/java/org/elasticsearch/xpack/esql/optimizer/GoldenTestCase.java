/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Listeners;

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
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
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
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;

/** See GoldenTestsReadme.md for more information about these tests. */
@Listeners({ GoldenTestCase.GoldenTestReproduceInfoPrinter.class })
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

    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, SearchStats searchStats, String... nestedPath) {
        String testName = extractTestName();
        new Test(baseFile, testName, nestedPath, esqlQuery, stages, searchStats).doTest();
    }

    protected TestBuilder builder(String esqlQuery) {
        return new TestBuilder(esqlQuery);
    }

    protected final class TestBuilder {
        private final String esqlQuery;
        private EnumSet<Stage> stages;
        private SearchStats searchStats;
        private String[] nestedPath;

        private TestBuilder(String esqlQuery, EnumSet<Stage> stages, SearchStats searchStats, String[] nestedPath) {
            this.esqlQuery = esqlQuery;
            this.stages = stages;
            this.searchStats = searchStats;
            this.nestedPath = nestedPath;
        }

        TestBuilder(String esqlQuery) {
            this(esqlQuery, EnumSet.allOf(Stage.class), EsqlTestUtils.TEST_SEARCH_STATS, new String[0]);
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

        public void run() {
            runGoldenTest(esqlQuery, stages, searchStats, nestedPath);
        }
    }

    private record Test(
        Path basePath,
        String testName,
        String[] nestedPath,
        String esqlQuery,
        EnumSet<Stage> stages,
        SearchStats searchStats
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
            TransportVersion version = TransportVersion.current();
            var analyzer = new Analyzer(
                new AnalyzerContext(
                    EsqlTestUtils.TEST_CFG,
                    new EsqlFunctionRegistry(),
                    CsvTests.loadIndexResolution(CsvTests.testDatasets(parsedStatement)),
                    defaultLookupResolution(),
                    new EnrichResolution(),
                    InferenceResolution.EMPTY,
                    version,
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
            var logicallyOptimized = new LogicalPlanOptimizer(unboundLogicalOptimizerContext()).optimize(analyzed);
            if (stages.contains(Stage.LOGICAL_OPTIMIZATION)) {
                result.add(Tuple.tuple(Stage.LOGICAL_OPTIMIZATION, verifyOrWrite(logicallyOptimized, Stage.LOGICAL_OPTIMIZATION)));
            }
            if (stages.contains(Stage.PHYSICAL_OPTIMIZATION) || stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)) {
                var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(null, null));
                PhysicalPlan physicalPlan = physicalPlanOptimizer.optimize(new Mapper().map(new Versioned<>(logicallyOptimized, version)));
                if (stages.contains(Stage.PHYSICAL_OPTIMIZATION)) {
                    result.add(Tuple.tuple(Stage.PHYSICAL_OPTIMIZATION, verifyOrWrite(physicalPlan, Stage.PHYSICAL_OPTIMIZATION)));
                }
                if (stages.contains(Stage.LOCAL_PHYSICAL_OPTIMIZATION)) {
                    Configuration conf = analyzer.context().configuration();
                    var localPhysicalPlan = PlannerUtils.localPlan(
                        PlannerSettings.DEFAULTS,
                        new EsqlFlags(false),
                        conf,
                        conf.newFoldContext(),
                        physicalPlan,
                        searchStats,
                        new PlanTimeProfile()
                    );
                    result.add(
                        Tuple.tuple(Stage.LOCAL_PHYSICAL_OPTIMIZATION, verifyOrWrite(localPhysicalPlan, Stage.LOCAL_PHYSICAL_OPTIMIZATION))
                    );
                }
            }
            return result;
        }

        enum TestResult {
            SUCCESS,
            FAILURE,
            CREATED
        }

        private <T extends QueryPlan<T>> TestResult verifyOrWrite(T plan, Stage stage) throws IOException {
            var outputPath = outputPath(stage);
            if (System.getProperty("golden.overwrite") != null) {
                logger.info("Bulldozing file {}", outputPath);
                return createNewOutput(outputPath, plan);
            } else {
                if (Files.exists(outputPath)) {
                    return verifyExisting(outputPath, plan);
                } else {
                    logger.debug("No output exists for file {}, writing new output", outputPath);
                    return createNewOutput(outputPath, plan);
                }
            }
        }

        private Path outputPath(Stage stage) {
            var paths = new String[nestedPath.length + 2];
            paths[0] = testName;
            System.arraycopy(nestedPath, 0, paths, 1, nestedPath.length);
            paths[paths.length - 1] = Strings.format("%s.expected", stage.name().toLowerCase(Locale.ROOT));
            return PathUtils.get(basePath.toString(), paths);
        }
    }

    private static Test.TestResult createNewOutput(Path output, QueryPlan<?> plan) throws IOException {
        Files.createDirectories(output.getParent());
        Files.writeString(output, toString(plan), StandardCharsets.UTF_8);
        return Test.TestResult.CREATED;
    }

    private static String toString(Node<?> plan) {
        return IDENTIFIER_PATTERN.matcher(plan.toString(Node.NodeStringFormat.FULL)).replaceAll("");
    }

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("#\\d+");

    private static Test.TestResult verifyExisting(Path output, QueryPlan<?> plan) throws IOException {
        String read = Files.readString(output);
        String testString = normalize(toString(plan));
        if (normalize(testString).equals(normalize(read))) {
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

    protected enum Stage {
        ANALYSIS,
        LOGICAL_OPTIMIZATION,
        PHYSICAL_OPTIMIZATION,
        // There's no LOCAL_LOGICAL here since in production we use PlannerUtils.localPlan to produce the local physical plan directly from
        // non-local physical plan.
        LOCAL_PHYSICAL_OPTIMIZATION
    }

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
