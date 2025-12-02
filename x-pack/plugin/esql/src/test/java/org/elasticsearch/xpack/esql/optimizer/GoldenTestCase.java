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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTests;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;

/** See GoldenTestsReadme.md for more information about these tests. */
@Listeners({ GoldenTestReproduceInfoPrinter.class })
public abstract class GoldenTestCase extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(GoldenTestCase.class);

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
        File baseFile,
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
                    version
                ),
                TEST_VERIFIER
            );
            List<Tuple<Stage, TestResult>> result = new ArrayList<>();
            var analyzed = analyzer.analyze(parsedStatement);
            if (stages.contains(Stage.ANALYZER)) {
                result.add(Tuple.tuple(Stage.ANALYZER, verifyOrWrite(analyzed, Stage.ANALYZER)));
            }
            if (stages.equals(EnumSet.of(Stage.ANALYZER))) {
                return result;
            }
            var logicallyOptimized = new LogicalPlanOptimizer(unboundLogicalOptimizerContext()).optimize(analyzed);
            if (stages.contains(Stage.LOGICAL)) {
                result.add(Tuple.tuple(Stage.LOGICAL, verifyOrWrite(logicallyOptimized, Stage.LOGICAL)));
            }
            if (stages.contains(Stage.PHYSICAL) || stages.contains(Stage.LOCAL_PHYSICAL)) {
                var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(null, null));
                PhysicalPlan physicalPlan = physicalPlanOptimizer.optimize(new Mapper().map(new Versioned<>(logicallyOptimized, version)));
                if (stages.contains(Stage.PHYSICAL)) {
                    result.add(Tuple.tuple(Stage.PHYSICAL, verifyOrWrite(physicalPlan, Stage.PHYSICAL)));
                }
                if (stages.contains(Stage.LOCAL_PHYSICAL)) {
                    Configuration conf = analyzer.context().configuration();
                    var localPhysicalPlan = PlannerUtils.localPlan(
                        EsqlTestUtils.TEST_PLANNER_SETTINGS,
                        new EsqlFlags(false),
                        conf,
                        conf.newFoldContext(),
                        physicalPlan,
                        searchStats,
                        new PlanTimeProfile()
                    );
                    result.add(Tuple.tuple(Stage.LOCAL_PHYSICAL, verifyOrWrite(localPhysicalPlan, Stage.LOCAL_PHYSICAL)));
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
            var outputFile = outputFile(stage);
            if (System.getProperty("golden.bulldoze") != null) {
                logger.info("Bulldozing file {}", outputFile);
                return createNewOutput(outputFile, plan);
            } else {
                if (outputFile.toFile().exists()) {
                    return verifyExisting(outputFile, plan);
                } else {
                    logger.debug("No output exists for file {}, writing new output", outputFile);
                    return createNewOutput(outputFile, plan);
                }
            }
        }

        private Path outputFile(Stage stage) {
            var paths = new String[nestedPath.length + 2];
            paths[0] = testName;
            System.arraycopy(nestedPath, 0, paths, 1, nestedPath.length);
            paths[paths.length - 1] = Strings.format("%s.expected", stage.name().toLowerCase());
            return Path.of(baseFile.getAbsolutePath(), paths);
        }
    }

    private static Test.TestResult createNewOutput(Path output, QueryPlan<?> plan) throws IOException {
        Files.createDirectories(output.getParent());
        Files.writeString(output, plan.goldenTestToString(), StandardCharsets.UTF_8);
        return Test.TestResult.CREATED;
    }

    private static Test.TestResult verifyExisting(Path output, QueryPlan<?> plan) throws IOException {
        String read = Files.readString(output);
        String testString = normalize(plan.goldenTestToString());
        if (normalize(testString).equals(normalize(read))) {
            if (System.getProperty("golden.cleanactual") != null) {
                File file = getActualFile(output).toFile();
                if (file.exists()) {
                    logger.debug("Cleaning up actual file '%s' because golden.cleanactual property is set".formatted(file));
                    if (file.delete() == false) {
                        logger.warn("Could not delete actual file '%s'".formatted(file));
                    }
                }
            }
            return Test.TestResult.SUCCESS;
        }

        if (System.getProperty("golden.noactual") != null) {
            logger.debug("Skipping actual file creation because golden.noactual property is set");
        } else {
            List<String> actualLines = testString.lines().map(GoldenTestCase::normalize).toList();
            Path actualFile = getActualFile(output);
            logger.info("Creating actual file at " + actualFile.toAbsolutePath());
            Files.write(actualFile, actualLines);
        }
        return Test.TestResult.FAILURE;
    }

    private static Path getActualFile(Path output) {
        return output.resolveSibling(output.getFileName().toString().replaceAll("expected", "actual"));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private final File baseFile = new File(
        URI.create(
            new File(getClass().getResource(".").getFile()).toURI().toString().replaceAll("build/classes/java/test", "src/test/resources")
                + "/golden_tests/"
                + StringUtils.camelCaseToUnderscore(getClass().getSimpleName()).toLowerCase()
                + "/"
        )
    );

    protected enum Stage {
        ANALYZER,
        LOGICAL,
        PHYSICAL,
        // There's no LOCAL_LOGICAL here since in product we use PlannerUtils.localPlan to produce the local physical plan directly from
        // non-local physical plan.
        LOCAL_PHYSICAL
    }

    private static String normalize(String s) {
        return s.lines().map(String::strip).collect(Collectors.joining("\n"));
    }

    private static String extractTestName() {
        for (var trace : Thread.currentThread().getStackTrace()) {
            // Search for a public void testX() method in a GoldenTestCase subclass
            if (trace.getMethodName().startsWith("test")) {
                var className = trace.getClassName();
                try {
                    var clazz = Class.forName(className);
                    if (GoldenTestCase.class.isAssignableFrom(clazz)) {
                        var method = clazz.getMethod(trace.getMethodName());
                        if (method.getReturnType() == void.class && Modifier.isPublic(method.getModifiers())) {
                            return StringUtils.camelCaseToUnderscore(trace.getMethodName().substring(4)).toLowerCase();
                        }
                    }
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new IllegalStateException("Could not extract test name from stack trace");
    }
}
