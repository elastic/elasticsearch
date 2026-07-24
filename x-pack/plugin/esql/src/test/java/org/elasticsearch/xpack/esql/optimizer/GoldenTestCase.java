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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
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
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
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
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;

/** See GoldenTestsReadme.md for more information about these tests. */
@Listeners({ GoldenTestCase.GoldenTestReproduceInfoPrinter.class })
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // ExtrasFS can create extraneous files in the output directory.
public abstract class GoldenTestCase extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(GoldenTestCase.class);

    /**
     * RandomizedRunner appends {@code {seed=[...]}} to {@link #getTestName()} for {@code -Dtests.iters} / {@code @Repeat} (See #144763),
     * and {@code {<params>}} for {@code @ParametersFactory}-parameterized suites. Neither may leak into the golden directory path,
     * which must stay stable across seeds and parameters.
     */
    private static final Pattern RANDOMIZED_RUNNER_SUFFIX_AT_END = Pattern.compile("(?:\\s+\\{[^}]*\\})+$");

    /**
     * Fixed sample probability that is used for all query approximation plans.
     * Normally it would be determined from subplan execution, but those are
     * not supported in the golden tests.
     */
    private static final double SAMPLE_PROBABILITY = 0.0123456789;

    /**
     * {@code {current}} checks the newest version range at {@link TransportVersion#current()}; {@code {historical}} checks
     * every range at a random defined version inside it. Two fixed values so muting one never disarms the other.
     * See GoldenTestsReadme.MD.
     */
    public static final String MODE_CURRENT = "current";
    public static final String MODE_HISTORICAL = "historical";

    /** What a parameterized golden suite's {@code @ParametersFactory} method returns. */
    public static Iterable<Object[]> goldenModes() {
        return List.of(new Object[] { MODE_CURRENT }, new Object[] { MODE_HISTORICAL });
    }

    private final Path baseFile;
    private final String goldenMode;

    public GoldenTestCase() {
        this(null);
    }

    protected GoldenTestCase(String goldenMode) {
        if (goldenMode != null && MODE_CURRENT.equals(goldenMode) == false && MODE_HISTORICAL.equals(goldenMode) == false) {
            throw new IllegalArgumentException("unknown golden mode [" + goldenMode + "]");
        }
        this.goldenMode = goldenMode;
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

    /**
     * Run a golden test where the query references views. {@code views} maps a view name to its definition query; the views are
     * registered on the analyzer and expanded (together with IN subqueries) before pre-analysis, mirroring
     * {@code EsqlSession#execute}.
     */
    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, Map<String, String> views, String... nestedPath) {
        builder(esqlQuery).stages(stages).views(views).nestedPath(nestedPath).run();
    }

    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, SearchStats searchStats, String... nestedPath) {
        builder(esqlQuery).stages(stages).searchStats(searchStats).nestedPath(nestedPath).run();
    }

    protected void runGoldenTest(String esqlQuery, EnumSet<Stage> stages, TransportVersion transportVersion) {
        builder(esqlQuery).stages(stages).transportVersion(transportVersion).run();
    }

    protected void runGoldenTest(
        String esqlQuery,
        EnumSet<Stage> stages,
        SearchStats searchStats,
        TransportVersion transportVersion,
        String... nestedPath
    ) {
        builder(esqlQuery).stages(stages).searchStats(searchStats).transportVersion(transportVersion).nestedPath(nestedPath).run();
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
        private boolean explicitTransportVersion;
        private TransportVersion since;
        private final List<Label> labels = new ArrayList<>();
        private Function<LogicalOptimizerContext, LogicalPlanOptimizer> optimizerFactory;
        private AliasFilter aliasFilter;
        private ProjectMetadata datasetMetadata;
        private ExternalSourceResolution externalSourceResolution = ExternalSourceResolution.EMPTY;
        private Map<String, String> views = Map.of();

        private TestBuilder(
            String esqlQuery,
            EnumSet<Stage> stages,
            SearchStats searchStats,
            String[] nestedPath,
            TransportVersion transportVersion,
            AliasFilter aliasFilter
        ) {
            this.esqlQuery = esqlQuery;
            this.stages = stages;
            this.searchStats = searchStats;
            this.nestedPath = nestedPath;
            this.transportVersion = transportVersion;
            this.aliasFilter = aliasFilter;
        }

        TestBuilder(String esqlQuery) {
            this(esqlQuery, EnumSet.allOf(Stage.class), EsqlTestUtils.TEST_SEARCH_STATS, new String[0], null, null);
        }

        public TestBuilder optimizer(Function<LogicalOptimizerContext, LogicalPlanOptimizer> factory) {
            this.optimizerFactory = factory;
            return this;
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

        /**
         * Pins every run of this test to exactly this version — mutually exclusive with {@link #since} and
         * {@link #expectationChangesAt}, which describe a version window instead of a point.
         *
         * @deprecated declare the version dependence with {@link #since} / {@link #expectationChangesAt}; the pin
         *             disappears once existing suites have migrated.
         */
        @Deprecated
        public TestBuilder transportVersion(TransportVersion transportVersion) {
            this.transportVersion = transportVersion;
            this.explicitTransportVersion = true;
            return this;
        }

        /**
         * Lower-bounds the versions this test covers — coverage below is dropped, visibly. Use the transport version of
         * the feature under test. Distinct from {@link #expectationChangesAt}, which splits coverage instead of removing it.
         */
        public TestBuilder since(String transportVersionName) {
            return since(resolve(transportVersionName));
        }

        /**
         * Same as {@link #since(String)}, for callers that already hold the version constant — which is why,
         * unlike the string form, no resolution check happens here.
         */
        public TestBuilder since(TransportVersion since) {
            this.since = since;
            return this;
        }

        /**
         * Declares a transport version at which this test's expected plan changes. Each declared cut-point starts a new
         * version range with its own expected files, in a subdirectory named after the version. Declare oldest-first.
         */
        public TestBuilder expectationChangesAt(String transportVersionName) {
            labels.add(new Label(transportVersionName, resolve(transportVersionName)));
            return this;
        }

        /** A retired transport version name means the code path it gated is gone — the declaration must go with it. */
        private static TransportVersion resolve(String transportVersionName) {
            try {
                return TransportVersion.fromName(transportVersionName);
            } catch (IllegalStateException e) {
                throw new IllegalStateException(
                    Strings.format(
                        "golden version declaration [%s] no longer resolves — its transport version was retired, so the planning "
                            + "path it covered no longer exists. Remove this declaration; for a label, also delete its "
                            + "[before_%s] directory. See GoldenTestsReadme.MD.",
                        transportVersionName,
                        transportVersionName
                    ),
                    e
                );
            }
        }

        public TestBuilder aliasFilter(AliasFilter aliasFilter) {
            this.aliasFilter = aliasFilter;
            return this;
        }

        public AliasFilter aliasFilter() {
            return aliasFilter;
        }

        /**
         * Registers external datasets (a {@link ProjectMetadata} carrying the data-source / dataset definitions) so that
         * {@code FROM <dataset>} references in the query are rewritten into external relations by {@link DatasetRewriter}, mirroring
         * {@code EsqlSession}. Must be paired with {@link #externalSourceResolution} so the analyzer can resolve those relations' schemas.
         */
        public TestBuilder datasetMetadata(ProjectMetadata datasetMetadata) {
            this.datasetMetadata = datasetMetadata;
            return this;
        }

        /** Pre-resolved schemas for the external datasets registered via {@link #datasetMetadata}. */
        public TestBuilder externalSourceResolution(ExternalSourceResolution externalSourceResolution) {
            this.externalSourceResolution = externalSourceResolution;
            return this;
        }

        public ExternalSourceResolution externalSourceResolution() {
            return externalSourceResolution;
        }

        public TestBuilder views(Map<String, String> views) {
            this.views = views;
            return this;
        }

        public void run() {
            String testName = RANDOMIZED_RUNNER_SUFFIX_AT_END.matcher(getTestName()).replaceFirst("");
            if (explicitTransportVersion && (since != null || labels.isEmpty() == false)) {
                throw new IllegalStateException("since()/expectationChangesAt() describe a version window; a pinned version has none");
            }
            if (since != null && labels.isEmpty() == false && labels.getFirst().version().id() <= since.id()) {
                throw new IllegalArgumentException(Strings.format("label [%s] must be above since [%s]", labels.getFirst().name(), since));
            }
            List<VersionRange> ranges = explicitTransportVersion
                ? List.of(new VersionRange(null, transportVersion, List.of(transportVersion)))
                : liveRanges(testName);
            try {
                Set<String> written = new HashSet<>();
                for (VersionRange range : ranges) {
                    if (overwriteMode() || hasExpectedFiles(outputDir(testName, range.dir())) == false) {
                        writeReference(testName, range);
                        written.add(String.valueOf(range.dir()));
                    }
                }
                List<String> failures = new ArrayList<>();
                for (Tuple<VersionRange, TransportVersion> check : checkPlan(ranges)) {
                    if (written.contains(String.valueOf(check.v1().dir()))) {
                        continue; // just written from these same plans — nothing to compare yet
                    }
                    List<Stage> failed = failedStages(test(testName, check.v1().dir(), check.v2(), false).doTests());
                    if (failed.isEmpty() == false) {
                        failures.add(Strings.format("%sstages %s at version [%s]", rangePrefix(check.v1()), failed, check.v2()));
                    }
                }
                if (failures.isEmpty() == false) {
                    fail(Strings.format("Output for test '%s' does not match: %s", testName, failures));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean undeclared() {
            return since == null && labels.isEmpty();
        }

        /** Ranges whose window can still be sampled; a dead range is a cleanup prompt, not a failure. */
        private List<VersionRange> liveRanges(String testName) {
            TransportVersion lowerBound = since != null ? since : TransportVersion.minimumCompatible();
            List<VersionRange> ranges = deriveRanges(lowerBound, labels, COMPATIBLE_VERSIONS);
            List<VersionRange> live = new ArrayList<>(ranges.size());
            for (int i = 0; i < ranges.size(); i++) {
                if (ranges.get(i).versions().isEmpty()) {
                    if (i >= labels.size()) {
                        // the newest range always samples current(); an empty one means the version bookkeeping broke
                        throw new IllegalStateException(
                            Strings.format("test [%s]: the newest golden range has no sampled versions", testName)
                        );
                    }
                    // dead ranges form the oldest prefix, so the boundary that lost its meaning is labels[i]
                    reportDeadRange(testName, ranges.get(i), labels.get(i).name());
                } else {
                    live.add(ranges.get(i));
                }
            }
            return live;
        }

        private void reportDeadRange(String testName, VersionRange range, String nextLabel) {
            String message = Strings.format(
                "test [%s]: golden range [%s] is dead — every version that can be sampled is past [%s]. "
                    + "Remove expectationChangesAt(\"%s\") and delete the [%s] directory. See GoldenTestsReadme.MD.",
                testName,
                range.dir(),
                nextLabel,
                nextLabel,
                range.dir()
            );
            if (System.getProperty("golden.gc.strict") != null) {
                fail(message);
            } else {
                logger.warn(message);
            }
        }

        /**
         * Writes the range's files at its anchor, then re-plans at the opposite end of the sampled window and fails on a
         * mismatch: a fork inside the range must be declared, not regenerated over.
         */
        private void writeReference(String testName, VersionRange range) throws IOException {
            TransportVersion anchor = explicitTransportVersion == false && undeclared() ? TransportVersion.current() : range.start();
            test(testName, range.dir(), anchor, true).doTests();
            TransportVersion guard = undeclared() ? range.versions().getFirst() : range.versions().getLast();
            if (guard.equals(anchor)) {
                return;
            }
            List<Stage> mismatched = failedStages(test(testName, range.dir(), guard, false).doTests());
            if (mismatched.isEmpty() == false) {
                fail(
                    Strings.format(
                        "test [%s]: %sfiles were written at [%s] but planning at [%s] disagrees for stages %s — the plan forks "
                            + "inside the version window these files cover. Declare expectationChangesAt(\"<version name>\") to keep "
                            + "the old shape covered, or since(\"<version name>\") to drop coverage below the fork. "
                            + "See GoldenTestsReadme.MD.",
                        testName,
                        rangePrefix(range),
                        anchor,
                        guard,
                        mismatched
                    )
                );
            }
        }

        private List<Tuple<VersionRange, TransportVersion>> checkPlan(List<VersionRange> ranges) {
            if (explicitTransportVersion) {
                return List.of(Tuple.tuple(ranges.getFirst(), transportVersion));
            }
            if (MODE_CURRENT.equals(goldenMode)) {
                return List.of(Tuple.tuple(ranges.getLast(), TransportVersion.current()));
            }
            if (MODE_HISTORICAL.equals(goldenMode)) {
                return ranges.stream().map(r -> Tuple.tuple(r, randomFrom(r.versions()))).toList();
            }
            // unmigrated suite: one draw, uniform across the whole sampled window
            List<Tuple<VersionRange, TransportVersion>> union = ranges.stream()
                .flatMap(r -> r.versions().stream().map(v -> Tuple.tuple(r, v)))
                .toList();
            return List.of(randomFrom(union));
        }

        private Test test(String testName, String rangeDir, TransportVersion version, boolean writeReference) {
            return new Test(
                baseFile,
                testName,
                nestedPath,
                rangeDir,
                esqlQuery,
                stages,
                searchStats,
                version,
                writeReference,
                optimizerFactory,
                aliasFilter,
                datasetMetadata,
                externalSourceResolution,
                views
            );
        }

        private Path outputDir(String testName, String rangeDir) {
            String[] parts = new String[nestedPath.length + (rangeDir == null ? 1 : 2)];
            parts[0] = testName;
            System.arraycopy(nestedPath, 0, parts, 1, nestedPath.length);
            if (rangeDir != null) {
                parts[parts.length - 1] = rangeDir;
            }
            return PathUtils.get(baseFile.toString(), parts);
        }

        private static List<Stage> failedStages(List<Tuple<Stage, Test.TestResult>> results) {
            return results.stream().filter(r -> r.v2() == Test.TestResult.FAILURE).map(Tuple::v1).toList();
        }

        private static String rangePrefix(VersionRange range) {
            return range.dir() == null ? "" : "range [" + range.dir() + "] ";
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

    record Label(String name, TransportVersion version) {}

    /**
     * {@code dir} is null for the newest range and for tests without labels — their files live directly in the test
     * directory. {@code versions} holds every supplied compatible version the range covers, ascending; checks draw from it.
     */
    record VersionRange(String dir, TransportVersion start, List<TransportVersion> versions) {}

    /**
     * Splits the version window at the labels and assigns every candidate version to the range it belongs to,
     * by {@link TransportVersion#supports}. Ranges may come back empty ({@code versions}) — the caller decides
     * whether that's a cleanup prompt or a failure.
     */
    static List<VersionRange> deriveRanges(TransportVersion lowerBound, List<Label> labels, Collection<TransportVersion> candidates) {
        List<TransportVersion> cuts = new ArrayList<>(labels.size() + 1);
        cuts.add(lowerBound);
        for (Label label : labels) {
            // declaration order = version order, so a misordering never silently builds wrong ranges
            if (cuts.size() > 1 && label.version().id() <= cuts.getLast().id()) {
                throw new IllegalArgumentException(
                    Strings.format("expectationChangesAt labels must be declared oldest-first: [%s] is out of order", label.name())
                );
            }
            cuts.add(label.version());
        }
        List<List<TransportVersion>> partition = new ArrayList<>(cuts.size());
        for (int i = 0; i < cuts.size(); i++) {
            partition.add(new ArrayList<>());
        }
        for (TransportVersion version : candidates) {
            if (version.supports(cuts.getFirst()) == false) {
                continue; // below the lower bound
            }
            int range = 0;
            for (int i = cuts.size() - 1; i > 0; i--) {
                if (version.supports(cuts.get(i))) {
                    range = i;
                    break;
                }
            }
            rejectStraddler(version, cuts, range);
            partition.get(range).add(version);
        }
        List<VersionRange> ranges = new ArrayList<>(cuts.size());
        for (int i = 0; i < cuts.size(); i++) {
            // named by the exclusive upper bound: when the floor passes a label, [before_<label>] is exactly what dies
            String dir = i < labels.size() ? "before_" + labels.get(i).name() : null;
            partition.get(i).sort(Comparator.naturalOrder());
            ranges.add(new VersionRange(dir, cuts.get(i), List.copyOf(partition.get(i))));
        }
        return ranges;
    }

    /**
     * A version supporting a newer cut but not an older one means a version-aware change was backported below another
     * one that wasn't. No range's expectations can be correct for such a version, so this is an error, not a skip:
     * it fails deterministically on the PR that registers the interfering backport id.
     */
    private static void rejectStraddler(TransportVersion version, List<TransportVersion> cuts, int range) {
        for (int i = 1; i < range; i++) {
            if (version.supports(cuts.get(i)) == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "transport version [%s] supports [%s] but not the older [%s] — a version-aware change was "
                            + "backported below another one that wasn't. No golden version range can describe this version's "
                            + "planning; see the backporting caution on Versioned's javadoc. If the backport is deliberate, "
                            + "cover this combination with explicitly pinned transport versions.",
                        version,
                        cuts.get(range),
                        cuts.get(i)
                    )
                );
            }
        }
    }

    /** Every defined compatible version: any of them may identify a running cluster, particularly on Serverless. */
    private static final List<TransportVersion> COMPATIBLE_VERSIONS = TransportVersionUtils.allReleasedVersions()
        .stream()
        .filter(TransportVersion::isCompatible)
        .toList();

    private static boolean overwriteMode() {
        return System.getProperty("golden.overwrite") != null;
    }

    private static boolean hasExpectedFiles(Path dir) throws IOException {
        if (Files.notExists(dir)) {
            return false;
        }
        try (var files = Files.list(dir)) {
            return files.anyMatch(f -> f.getFileName().toString().endsWith(".expected"));
        }
    }

    private record Test(
        Path basePath,
        String testName,
        String[] nestedPath,
        String rangeDir,
        String esqlQuery,
        EnumSet<Stage> stages,
        SearchStats searchStats,
        TransportVersion transportVersion,
        boolean writeReference,
        Function<LogicalOptimizerContext, LogicalPlanOptimizer> optimizerFactory,
        AliasFilter aliasFilter,
        ProjectMetadata datasetMetadata,
        ExternalSourceResolution externalSourceResolution,
        Map<String, String> views
    ) {

        private List<Tuple<Stage, TestResult>> doTests() throws IOException {
            EsqlStatement statement = TEST_PARSER.createStatement(esqlQuery);
            // Mirror EsqlSession#execute: expand views and rewrite IN subqueries into SemiJoin/AntiJoin/MarkJoin before
            // running pre-analysis and analysis, so inner subquery indices are discovered and verifier checks (e.g. unbounded
            // SORT inside an IN subquery) fire. When the query references views, register them and run the iterative
            // view/IN-subquery resolution; otherwise resolve IN subqueries only.
            LogicalPlan parsedPlan;
            if (views.isEmpty()) {
                parsedPlan = InSubqueryResolver.resolve(statement.plan());
            } else {
                TestAnalyzer viewAnalyzer = analyzer();
                views.forEach(viewAnalyzer::addView);
                parsedPlan = viewAnalyzer.resolveViewsAndInSubqueries(statement.plan());
            }
            // Then turn FROM <dataset> targets into UnresolvedExternalRelation, exactly as EsqlSession does. A
            // null datasetMetadata (the default) makes this a no-op, so plain golden tests are unaffected; when a
            // test registers datasets, external relations are excluded from CSV index discovery below.
            parsedPlan = DatasetRewriter.rewriteUnsecured(parsedPlan, datasetMetadata, TestIndexNameExpressionResolver.newInstance());
            String[] queryPathParts = new String[nestedPath.length + 2];
            queryPathParts[0] = testName;
            System.arraycopy(nestedPath, 0, queryPathParts, 1, nestedPath.length);
            queryPathParts[queryPathParts.length - 1] = "query.esql";
            Path queryPath = PathUtils.get(basePath.toString(), queryPathParts);
            Files.createDirectories(queryPath.getParent());
            Files.writeString(queryPath, esqlQuery);
            if (rangeDir != null) {
                Files.createDirectories(queryPath.getParent().resolve(rangeDir));
            }
            UnmappedResolution unmappedResolution = statement.setting(UNMAPPED_FIELDS);
            TestAnalyzer testAnalyzer = analyzer().addLanguagesLookup()
                .addTestLookup()
                .addMultiColumnJoinableLookup()
                .addAnalysisTestsEnrichResolution()
                .addAnalysisTestsInferenceResolution()
                .minimumTransportVersion(transportVersion)
                .externalSourceResolution(externalSourceResolution)
                .unmappedResolution(unmappedResolution);
            boolean trackUnmappedFieldIndices = unmappedResolution == UnmappedResolution.LOAD;
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
            var optimizer = optimizerFactory != null
                ? optimizerFactory.apply(optimizerContext)
                : new LogicalPlanOptimizer(optimizerContext);
            var logicallyOptimized = optimizer.optimize(analyzed);
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
                            PhysicalPlan lookupPhysical = optimizeLookupPhysicalPlan(
                                lookupLogical,
                                configuration,
                                searchStats,
                                aliasFilter
                            );
                            TestResult r = verifyOrWrite(lookupPhysical, outputPath("lookup_physical_optimization" + suffix));
                            result.add(Tuple.tuple(Stage.LOOKUP_PHYSICAL_OPTIMIZATION, r));
                        }
                    }
                }
                if (stages.contains(Stage.NODE_REDUCE) || stages.contains(Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION)) {
                    List<ExchangeExec> exchanges = physicalPlan.collect(ExchangeExec.class);
                    // Skip plans that terminate at the
                    // coordinator and produce no ExchangeExec;
                    // e.g. query that optimized data scan entirely like `time()`

                    if (exchanges.isEmpty() == false) {
                        ExchangeExec exec = EsqlTestUtils.singleValue(exchanges);
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
                                addNodeReduceDualPlanResult(
                                    reductionPlan,
                                    dualFileOutput.nodeReduceOutput(),
                                    dualFileOutput.dataNodeOutput()
                                )
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
            if (writeReference) {
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
            var paths = new String[nestedPath.length + (rangeDir == null ? 2 : 3)];
            paths[0] = testName;
            System.arraycopy(nestedPath, 0, paths, 1, nestedPath.length);
            if (rangeDir != null) {
                paths[paths.length - 2] = rangeDir;
            }
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

        private static PhysicalPlan optimizeLookupPhysicalPlan(
            LogicalPlan logicalPlan,
            Configuration conf,
            SearchStats stats,
            AliasFilter aliasFilter
        ) {
            FoldContext foldCtx = conf.newFoldContext();
            PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(logicalPlan);
            var context = new LookupPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                conf,
                foldCtx,
                stats,
                aliasFilter
            );
            return new LookupPhysicalPlanOptimizer(context).optimize(physicalPlan);
        }
    }

    private static Test.TestResult createNewOutput(Path output, QueryPlan<?> plan) throws IOException {
        if (output.getFileName().toString().contains("extra")) {
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
        Map<String, IndexMode> indexModes = datasets.datasets()
            .stream()
            .collect(Collectors.toMap(CsvTestsDataLoader.TestDataset::indexName, GoldenTestCase::indexModeOf));
        List<MappingPerIndex> mappings = datasets.datasets()
            .stream()
            .map(ds -> new MappingPerIndex(ds.indexName(), createMappingForIndex(ds)))
            .toList();
        var mergedMappings = mergeMappings(mappings, trackUnmappedFieldIndices);
        return IndexResolution.valid(new EsIndex(datasets.indexPattern(), mergedMappings.mapping, indexModes, Map.of(), Map.of()));
    }

    /**
     * The dataset's real index mode, read from its settings file (e.g. k8s-settings.json declares
     * {@code index.mode: time_series}) rather than assumed - a golden test's query can reference the
     * same index via TS or FROM, and TS requires every matched index to genuinely be time-series
     * mode (see https://github.com/elastic/elasticsearch/issues/153030), so this must reflect the
     * dataset's actual settings, not a blanket default.
     */
    private static IndexMode indexModeOf(CsvTestsDataLoader.TestDataset dataset) {
        try {
            return IndexSettings.MODE.get(dataset.loadSettings());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    private record MappingPerIndex(String index, Map<String, EsField> mapping) {}

    private record MergedResult(Map<String, EsField> mapping) {}

    private static MergedResult mergeMappings(List<MappingPerIndex> mappingsPerIndex, boolean trackUnmappedFieldIndices) {
        Map<String, Map<String, EsField>> fieldNamesToFieldByIndices = new HashMap<>();
        for (var mappingPerIndex : mappingsPerIndex) {
            for (var entry : mappingPerIndex.mapping().entrySet()) {
                fieldNamesToFieldByIndices.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                    .put(mappingPerIndex.index(), entry.getValue());
            }
        }
        int numberOfIndices = mappingsPerIndex.size();
        Map<String, EsField> mappings = new HashMap<>();
        for (var entry : fieldNamesToFieldByIndices.entrySet()) {
            String fieldName = entry.getKey();
            mappings.put(fieldName, mergeFields(fieldName, fieldName, entry.getValue(), trackUnmappedFieldIndices, numberOfIndices));
        }
        return new MergedResult(mappings);
    }

    private static EsField mergeFields(
        String fieldName,
        String fullName,
        Map<String, EsField> fieldByIndex,
        boolean trackUnmappedFieldIndices,
        int numberOfIndices
    ) {
        EsField field;
        if (fieldByIndex.values().stream().map(EsField::getDataType).distinct().count() > 1) {
            field = new InvalidMappedField(fieldName, getTypesToIndices(fieldByIndex));
        } else {
            // We take scalar attributes (name, dataType, aggregatable, timeSeriesFieldType) from an arbitrary representative.
            // This is safe because: dataType is already verified identical above, name is the map key, and the only fields
            // that reach this path are OBJECT parents whose children differ; objects are never aggregatable and always have
            // TimeSeriesFieldType.NONE (time series types are set on leaf fields, not parent objects).
            List<EsField> fields = fieldByIndex.values().stream().distinct().limit(2).toList();
            EsField representative = fields.getFirst();
            if (fields.size() == 1) {
                field = representative;
            } else {
                Map<String, EsField> mergedChildren = mergeSubFields(
                    fullName,
                    getSubNameToIndexToSubField(fieldByIndex),
                    trackUnmappedFieldIndices,
                    numberOfIndices
                );
                field = new EsField(
                    representative.getName(),
                    representative.getDataType(),
                    mergedChildren,
                    representative.isAggregatable(),
                    representative.getTimeSeriesFieldType()
                );
            }
        }
        return trackUnmappedFieldIndices
            ? IndexResolver.wrapIfPartiallyUnmapped(field, fieldName, fullName, fieldByIndex.keySet(), numberOfIndices)
            : field;
    }

    /** Returns {@code Map<SubName, Map<IndexName, EsField>>}; where are typedefs when you need them! */
    private static Map<String, Map<String, EsField>> getSubNameToIndexToSubField(Map<String, EsField> fieldByIndex) {
        Map<String, Map<String, EsField>> result = new HashMap<>();
        for (var entry : fieldByIndex.entrySet()) {
            String index = entry.getKey();
            for (var property : entry.getValue().getProperties().entrySet()) {
                result.computeIfAbsent(property.getKey(), k -> new HashMap<>()).put(index, property.getValue());
            }
        }
        return result;
    }

    private static Map<String, EsField> mergeSubFields(
        String parentFullName,
        Map<String, Map<String, EsField>> subFieldsByIndexBySubName,
        boolean trackUnmappedFieldIndices,
        int numberOfIndices
    ) {
        Map<String, EsField> properties = new TreeMap<>();
        for (var subEntry : subFieldsByIndexBySubName.entrySet()) {
            String subName = subEntry.getKey();
            properties.put(
                subName,
                mergeFields(subName, parentFullName + "." + subName, subEntry.getValue(), trackUnmappedFieldIndices, numberOfIndices)
            );
        }
        return properties;
    }

    /** Returns {@code Map<TypeName, Set<IndexName>>}; where are typedefs when you need them! */
    private static Map<String, Set<String>> getTypesToIndices(Map<String, EsField> fieldByIndex) {
        var result = new HashMap<String, Set<String>>();
        for (var entry : fieldByIndex.entrySet()) {
            result.computeIfAbsent(entry.getValue().getDataType().typeName(), k -> new HashSet<>()).add(entry.getKey());
        }
        return result;
    }
}
