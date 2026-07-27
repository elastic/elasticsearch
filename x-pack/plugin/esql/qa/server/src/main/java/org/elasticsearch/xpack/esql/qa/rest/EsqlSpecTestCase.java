/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpEntity;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvAssert;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.Mode;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.RequestObjectBuilder;
import org.elasticsearch.xpack.esql.telemetry.TookMetrics;
import org.elasticsearch.xpack.esql.view.RestPutViewAction;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.CsvAssert.assertDataWithValueConverter;
import static org.elasticsearch.xpack.esql.CsvAssert.assertMetadata;
import static org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import static org.elasticsearch.xpack.esql.CsvTestUtils.assumeFalseLogging;
import static org.elasticsearch.xpack.esql.CsvTestUtils.assumeTrueLogging;
import static org.elasticsearch.xpack.esql.CsvTestUtils.csvFileTemplateResolver;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestUtils.substituteTemplates;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.createInferenceEndpoints;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadViewsIntoEs;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.COMPLETION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EMBEDDING_FUNCTION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.KNN_FUNCTION_V5;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.RERANK;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SOURCE_FIELD_MAPPING;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.VIEWS_CRUD_AS_INDEX_ACTIONS;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.assertNotPartial;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.junit.Assume.assumeFalse;

// Each generated variant class runs all csv-spec files in one category-sorted pass; that single-class suite completes
// well within 10 minutes (the whole multi-class run is the sum of these). This is a per-suite (per-class) timeout.
@TimeoutSuite(millis = 10 * TimeUnits.MINUTE)
public abstract class EsqlSpecTestCase extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private static final Logger LOGGER = LogManager.getLogger(EsqlSpecTestCase.class);
    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    protected final CsvTestCase testCase;
    protected final String instructions;
    protected final Mode mode;
    protected static Boolean supportsTook;
    protected static Boolean supportsViews;

    public static final Map<String, String> LOGGING_CLUSTER_SETTINGS = Map.of(
        // additional logging for https://github.com/elastic/elasticsearch/issues/139262 investigation
        "logger.org.elasticsearch.compute.operator.ChangePointOperator",
        "DEBUG",
        "logger.org.elasticsearch.xpack.esql.expression.function.scalar.convert",
        "TRACE"
    );

    /**
     * All csv-spec test cases, ordered by category so the whole suite runs one category at a time in a single JVM.
     * The category-grouped order is essential: it lets the cluster's per-category data be set up once per category
     * rather than thrashing (see {@link #ensureCategoryLoaded}); the generated {@code @ParametersFactory} passes
     * {@code shuffle = false} so this order is preserved (the default shuffles it).
     *
     * <p>Categories are ordered by ascending declared index count (so the empty {@code norows} category runs first and
     * the large {@code core} category runs last). Combined with the delta loader in {@link #ensureCategoryLoaded}, this
     * makes most category switches additive — indices are mostly created as later categories need them, with few
     * deletes — and avoids tearing everything down for the empty category in the middle of the run.
     *
     * <p>This is the shared parameter source (a "hook", not itself a {@code @ParametersFactory}). Each generated
     * variant class declares a {@code @ParametersFactory readScriptSpec()} whose body calls this method by its
     * <em>unqualified</em> name, so Java static-method hiding lets a variant base (e.g.
     * {@code AbstractEsqlSpecForceStoredLoadingIT}) substitute a filtered version transparently.
     */
    public static List<Object[]> csvSpecParameters() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertTrue("Not enough specs found " + urls, urls.size() > 0);
        List<Object[]> specs = new ArrayList<>(SpecReader.readScriptSpec(urls, CsvSpecReader::specParser));
        // Group tests so every category is contiguous, ordered by ascending declared index count (then category name
        // for a stable order, then file and line). The declared count is used (not the availability-filtered count) so
        // the order is identical across suites. See categoryFor: every file maps to exactly one category.
        specs.sort(
            Comparator.<Object[]>comparingInt(spec -> CsvTestsDataLoader.categoryFor((String) spec[1]).indices().size())
                .thenComparing(spec -> CsvTestsDataLoader.categoryFor((String) spec[1]).name())
                .thenComparing(spec -> (String) spec[1])
                .thenComparingInt(spec -> (Integer) spec[3])
        );
        return specs;
    }

    /**
     * Load test cases from a single named CSV spec file, e.g. {@code "/stats.csv-spec"}.
     */
    protected static List<Object[]> readScriptSpec(String specFile) throws Exception {
        URL url = classpathResource(specFile);
        assertNotNull("No resource found for " + specFile, url);
        return SpecReader.readScriptSpec(List.of(url), CsvSpecReader::specParser);
    }

    protected EsqlSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.instructions = instructions;
        this.mode = randomFrom(Mode.values());
    }

    protected static boolean testClustersOk = true;

    // The whole csv-spec suite runs in one JVM against one cluster. Tests are grouped by category (see the
    // category-sorted parameter source {@link #csvSpecParameters()}); when the running test's category differs from
    // the one the cluster currently holds, we tear down the previous category's data and load the new category's.
    // This keeps "FROM *" scoped to a category and views present only for the views category, without one cluster
    // per category.
    private static final Object CATEGORY_LOCK = new Object();
    private static volatile CsvTestsDataLoader.Category loadedCategory = null;
    // The index and view names currently loaded (the requested sets, before availability filtering), tracked so a
    // category switch only applies the delta rather than wiping and reloading. Mutated only under CATEGORY_LOCK.
    private static Set<String> loadedIndices = Set.of();
    private static Set<String> loadedViews = Set.of();

    @Before
    public void setup() throws IOException {
        assumeTrue("test clusters were broken", testClustersOk);
        ensureCategoryLoaded(category());
        // Skip tests entirely when the cluster cannot support the views their category needs: views are not loaded,
        // so running them would fail with "index not found" rather than giving a meaningful skip.
        if (viewsToLoad().isEmpty() == false) {
            assumeTrue(
                "Cluster does not support views (" + RestPutViewAction.VIEWS_PUT_SERVERLESS_SCOPE + " capability absent)",
                supportsViews()
            );
        }
    }

    /**
     * Ensures the cluster holds exactly this test's required data, switching from the previously loaded category if
     * needed by applying only the delta (create newly needed indices/views/enrich, delete no-longer-needed ones, leave
     * shared ones in place). No-op if the category is already loaded.
     *
     * <p>The delta is driven by {@link #indicesToLoad()}/{@link #viewsToLoad()} (the overridable requested sets), so
     * suites that opt out of category scoping (external-source, generative) work unchanged. Because the suite runs in
     * ascending index-count order (see {@link #csvSpecParameters()}), switches are mostly additive.
     */
    private void ensureCategoryLoaded(CsvTestsDataLoader.Category category) throws IOException {
        if (category.equals(loadedCategory)) {
            return;
        }
        synchronized (CATEGORY_LOCK) {
            if (category.equals(loadedCategory)) {
                return;
            }
            if (loadedCategory == null) {
                // First load in this JVM: one cluster is shared across the per-variant test classes, so start from a
                // known-clean slate. The subsequent delta then creates this category's data from empty.
                LOGGER.info("Loading first category [{}]: wiping any pre-existing data", category.name());
                CsvTestsDataLoader.deleteAllData(adminClient());
            } else {
                LOGGER.info("Category switch [{}] -> [{}]: applying data delta", loadedCategory.name(), category.name());
            }
            // Inference endpoints must exist before ingesting datasets that rely on them; creation is idempotent and
            // endpoints are not torn down between categories.
            createInferenceEndpointsIfSupported();

            Set<String> targetIndices = new HashSet<>(indicesToLoad());
            List<String> currentEnrich = loadedCategory != null ? loadedCategory.enrich() : List.of();
            CsvTestsDataLoader.syncIndicesAndEnrich(
                client(),
                supportsIndexModeLookup(),
                supportsSourceFieldMapping(),
                supportsSemanticTextInference(),
                timeSeriesOnly(),
                this::clusterHasCapability,
                loadedIndices,
                targetIndices,
                currentEnrich,
                category.enrich()
            );

            // Views delta (only the views category declares any; loaded through the admin client with a cap check).
            Set<String> targetViews = new HashSet<>(viewsToLoad());
            if (supportsViews()) {
                List<String> viewsToDelete = loadedViews.stream().filter(v -> targetViews.contains(v) == false).toList();
                if (viewsToDelete.isEmpty() == false) {
                    CsvTestsDataLoader.deleteViews(adminClient(), viewsToDelete);
                }
                List<String> viewsToCreate = targetViews.stream().filter(v -> loadedViews.contains(v) == false).toList();
                if (viewsToCreate.isEmpty() == false) {
                    loadViewsIntoEs(adminClient(), this::clusterHasCapability, viewsToCreate);
                }
            }

            loadedIndices = targetIndices;
            loadedViews = targetViews;
            loadedCategory = category;
        }
    }

    public boolean logResults() {
        return false;
    }

    public final void test() throws Throwable {
        try {
            shouldSkipTest(testName);
            doTest();
        } catch (Exception e) {
            ensureTestClustersAreOk(e);
            throw reworkException(e);
        }
    }

    protected void ensureTestClustersAreOk(Exception failure) {
        try {
            ensureHealth(client(), "", (request) -> {
                request.addParameter("wait_for_status", "yellow");
                request.addParameter("level", "shards");
            });
        } catch (Exception inner) {
            testClustersOk = false;
            failure.addSuppressed(inner);
        }
    }

    /**
     * The category (from the spec_data.yml manifest) this csv-spec file belongs to. Every file has exactly one
     * category (an unmapped file fails fast in {@link CsvTestsDataLoader#categoryFor}). Because the suite runs
     * category-sorted (see {@link #csvSpecParameters()}), all files of a category run contiguously, and the cluster is
     * loaded once per category as execution crosses category boundaries (see {@link #ensureCategoryLoaded}). Suites
     * that are not category-scoped (external-source, generative) override {@link #indicesToLoad()}/{@link #viewsToLoad()}.
     */
    protected CsvTestsDataLoader.Category category() {
        return CsvTestsDataLoader.categoryFor(groupName);
    }

    /**
     * Indices to load during setup: exactly the ones this file's category declares. Non-category-scoped suites
     * override this. Return {@code null} to load all indices, an empty list for none.
     */
    protected List<String> indicesToLoad() {
        return category().indices();
    }

    /** Views to load during setup: exactly the ones this file's category declares (empty for non-view categories). */
    protected Collection<String> viewsToLoad() {
        return category().views();
    }

    protected void shouldSkipTest(String testName) throws IOException {
        assumeTrueLogging("test clusters were broken", testClustersOk);
        if (requiresSemanticTextInference()) {
            assumeTrueLogging("Inference test service needs to be supported", supportsSemanticTextInference());
        }
        if (requiresInferenceEndpointOnLocalCluster()) {
            assumeTrueLogging("Inference test service needs to be supported", supportsInferenceTestServiceOnLocalCluster());
        }
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        if (testCase.requiredCapabilities.contains(VIEWS_CRUD_AS_INDEX_ACTIONS.capabilityName())) {
            assumeTrueLogging("Cluster does not support views", supportsViews());
        }
        assumeTrueLogging("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
        if (supportsSourceFieldMapping() == false) {
            assumeFalseLogging(
                "source mapping tests are muted",
                testCase.requiredCapabilities.contains(SOURCE_FIELD_MAPPING.capabilityName())
            );
        }
    }

    protected static void checkCapabilities(
        RestClient client,
        TestFeatureService testFeatureService,
        String testName,
        CsvTestCase testCase
    ) {
        checkCapabilities(client, testFeatureService, testName, testCase.requiredCapabilities);
        checkCapabilities(client, testFeatureService, testName, testCase.requiredCapabilitiesLocalCluster);
    }

    protected static void checkCapabilities(
        RestClient client,
        TestFeatureService testFeatureService,
        String testName,
        List<String> requiredCapabilities
    ) {
        if (hasCapabilities(client, requiredCapabilities)) {
            return;
        }

        var features = new EsqlFeatures().getFeatures().stream().map(NodeFeature::id).collect(Collectors.toSet());

        for (String feature : requiredCapabilities) {
            var esqlFeature = "esql." + feature;
            assumeTrueLogging("Requested capability " + feature + " is an ESQL cluster feature", features.contains(esqlFeature));
            assumeTrueLogging("Test " + testName + " requires " + feature, testFeatureService.clusterHasFeature(esqlFeature));
        }
    }

    protected boolean supportsSemanticTextInference() {
        return true;
    }

    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return true;
    }

    /**
     * Creates inference test endpoints when {@link #supportsInferenceTestServiceOnLocalCluster()} is true.
     * Subclasses may override to register a subset of endpoints for clusters that do not support all task types.
     */
    protected void createInferenceEndpointsIfSupported() throws IOException {
        if (supportsInferenceTestServiceOnLocalCluster()) {
            createInferenceEndpoints(adminClient());
        }
    }

    protected boolean requiresSemanticTextInference() {
        return testCase.requiredCapabilities.contains(SEMANTIC_TEXT_FIELD_CAPS.capabilityName());
    }

    protected boolean requiresInferenceEndpointOnLocalCluster() {
        return Stream.of(
            RERANK.capabilityName(),
            COMPLETION.capabilityName(),
            KNN_FUNCTION_V5.capabilityName(),
            TEXT_EMBEDDING_FUNCTION.capabilityName(),
            EMBEDDING_FUNCTION.capabilityName()
        ).anyMatch(testCase.requiredCapabilities::contains);
    }

    protected boolean timeSeriesOnly() {
        return Boolean.getBoolean("tests.esql.csv.timeseries_only");
    }

    protected boolean supportsIndexModeLookup() {
        return true;
    }

    protected boolean supportsSourceFieldMapping() {
        return true;
    }

    protected String maybeRandomizeQuery(String query) {
        return query;
    }

    /**
     * Intended to be used in {@link #maybeRandomizeQuery(String)} except in test cases that do not support {@code nullify}
     * (e.g. old test cases in bwc tests)
     */
    public String randomlyNullify(String query) {
        return randomBoolean()
            && testCase.expectedWarnings().isEmpty() // avoid shifting warnings positions in source query
            && testCase.expectedWarningsRegex().isEmpty() // regexp might also contain line/position
            && query.startsWith("SET") == false // avoid conflicts with provided settings
                ? "SET unmapped_fields=" + randomFrom("\"nullify\"; ", "\"default\"; ") + query
                : query;
    }

    /**
     * Returns true if the cluster under test supports the given ESQL capability.
     * Subclasses may override this to check additional clusters (e.g. remote clusters in CCS).
     */
    protected boolean clusterHasCapability(EsqlCapabilities.Cap capability) {
        return hasCapabilities(client(), List.of(capability.capabilityName()));
    }

    /**
     * Override in subclasses that support EXTERNAL; return the path used for path.repo.
     */
    protected Path getCsvDataPath() {
        return null;
    }

    protected void doTest() throws Throwable {
        // Dataset-backed specs (FROM <dataset>) need a registered data_source/dataset, which only the
        // external-source suites (AbstractExternalSourceSpecTestCase) provision. Plain spec subclasses
        // (single/multi-node, mixed-cluster, multi-cluster) share these csv-spec files via the
        // testFixtures classpath but have no fixture to back them, so skip rather than fail.
        assumeFalse(
            "dataset-backed spec; runs only on the external-source suites that register the dataset",
            testCase.datasetSources.isEmpty() == false
        );
        doTest(testCase.query);
    }

    protected final void doTest(String query) throws Throwable {
        if (query.trim().toUpperCase(Locale.ROOT).contains("EXTERNAL \"{{")) {
            // Multi-file glob templates ({{x_multifile}}, {{x_multifile_split}}, {{x_multifile_ubn}},
            // {{x_multifile_type_drift}}), hive-partitioned templates ({{x_hive}}), and ClickBench
            // templates ({{clickbench}}) are resolved by specialised subclasses against their own
            // fixtures. Plain EsqlSpecTestCase subclasses (mixed-cluster, multi-cluster,
            // single/multi-node, flight) share the same csv-spec files via the testFixtures classpath
            // but have no resolver for these templates, so skip such tests here.
            assumeFalseLogging(
                "specialised EXTERNAL templates require dedicated test subclass",
                query.contains("_multifile}}")
                    || query.contains("_multifile_split}}")
                    || query.contains("_multifile_ubn}}")
                    || query.contains("_multifile_type_drift}}")
                    || query.contains("_hive}}")
                    || query.contains("{{clickbench}}")
            );
            // external-multivalue.csv-spec exercises native multi-value reads for non-CSV/TSV format
            // ITs (Parquet/ORC/NDJSON/multi-node) which decode arrays from their format's native
            // representation. Its queries use {{employees}} without a multi_value_syntax opt-in
            // (the non-CSV format readers reject the unknown key via ConfigKeyValidator). On the
            // EsqlSpecTestCase cluster the local CSV reader defaults to multi_value_syntax: none
            // and would misalign columns on the bracket-MV employees.csv. CSV-side bracket-syntax
            // coverage lives in csv-multivalue.csv-spec with the explicit "brackets" opt-in.
            assumeFalseLogging(
                "external-multivalue requires AbstractExternalSourceSpecTestCase (native multi-value formats)",
                fileName.equals("external-multivalue.csv-spec")
            );
            Path path = getCsvDataPath();
            if (path != null) {
                query = substituteTemplates(query, csvFileTemplateResolver(path));
            }
        }
        query = maybeRandomizeQuery(query);

        RequestObjectBuilder builder = new RequestObjectBuilder(randomFrom(XContentType.values()));
        if (Strings.isNullOrEmpty(testCase.requestTimeRangeGte) == false) {
            String gte = testCase.requestTimeRangeGte;
            String lte = testCase.requestTimeRangeLte;
            builder.filter(b -> {
                b.startObject("range");
                b.startObject("@timestamp");
                b.field("gte", gte);
                b.field("lte", lte);
                b.endObject();
                b.endObject();
            });
        }

        boolean checkTook = supportsTook() && rarely();
        Map<?, ?> prevTooks = checkTook ? tooks() : null;

        addPragmas(builder);

        Map<String, Object> answer = RestEsqlTestCase.runEsql(
            builder.query(query),
            testCase.assertWarnings(deduplicateExactWarnings()),
            profileLogger,
            mode
        );

        assertNotPartial(answer);

        var expectedColumnsWithValues = loadCsvSpecValues(testCase.expectedResults);

        var metadata = answer.get("columns");
        assertNotNull(metadata);
        @SuppressWarnings("unchecked")
        var actualColumns = (List<Map<String, String>>) metadata;

        Logger logger = logResults() ? LOGGER : null;
        var values = answer.get("values");
        assertNotNull(values);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualValues = (List<List<Object>>) values;

        assertResults(expectedColumnsWithValues, actualColumns, actualValues, logger);
        if (testCase.expectedDocumentsFound != null) {
            assertTrue(
                "cluster is too old to assert returned document count",
                clusterHasCapability(EsqlCapabilities.Cap.DOCUMENTS_FOUND_AND_VALUES_LOADED)
            );
            CsvAssert.assertDocumentsFound(testCase.expectedDocumentsFound, (int) answer.get("documents_found"));
        }

        if (checkTook) {
            LOGGER.info("checking took incremented from {}", prevTooks);
            long took = ((Number) answer.get("took")).longValue();
            int prevTookHisto = ((Number) prevTooks.remove(tookKey(took))).intValue();
            assertMap(tooks(), matchesMap(prevTooks).entry(tookKey(took), prevTookHisto + 1));
        }
    }

    private void addPragmas(RequestObjectBuilder builder) throws IOException {
        MappedFieldType.FieldExtractPreference preference = fieldExtractPreference();
        Settings.Builder pragmaBuilder = Settings.builder();
        if (preference != null) {
            pragmaBuilder.put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), preference.toString()).build();
        }
        addRandomPragma(pragmaBuilder);
        testCase.pragmas.forEach(pragmaBuilder::put);

        Settings pragma = pragmaBuilder.build();
        if (pragma.isEmpty() == false) {
            builder.pragmas(pragma);
            builder.pragmasOk();
        }
    }

    /**
     * Add a random pragma to the request. Defaults to no-op
     */
    protected void addRandomPragma(Settings.Builder pragma) {
        if (randomBoolean() && hasCapabilities(client(), List.of("periodic_emit_partial_aggregation_results"))) {
            pragma.put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), between(10, 1000))
                .put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD.getKey(), randomDoubleBetween(0.1, 1.0, true));
        }
        if (enableRoundingDoubleValuesOnAsserting()
            && hasCapabilities(client(), List.of("auto_partition_docs_threshold"))
            && randomBoolean()) {
            pragma.put(PlannerSettings.DOC_THRESHOLD_AUTO_PARTITIONING.getKey(), between(1, 1000));
        }
    }

    protected MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return null;
    }

    private Map<?, ?> tooks() throws IOException {
        Request request = new Request("GET", "/_xpack/usage");
        HttpEntity entity = client().performRequest(request).getEntity();
        Map<?, ?> usage = XContentHelper.convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
        Map<?, ?> esql = (Map<?, ?>) usage.get("esql");
        return (Map<?, ?>) esql.get("took");
    }

    /**
     * Should warnings be de-duplicated before checking for exact matches. Defaults
     * to {@code false}, but in some environments we emit duplicate warnings. We'd prefer
     * not to emit duplicate warnings but for now it isn't worth fighting with. So! In
     * those environments we override this to deduplicate.
     * <p>
     *     Note: This only applies to warnings declared as {@code warning:}. Those
     *     declared as {@code warningRegex:} are always a list of
     *     <strong>allowed</strong> warnings. {@code warningRegex:} matches 0 or more
     *     warnings. There is no need to deduplicate because there's no expectation
     *     of an exact match.
     * </p>
     *
     */
    protected boolean deduplicateExactWarnings() {
        return false;
    }

    /**
     * Should the test ignore the order of individual values.
     */
    protected boolean ignoreValueOrder() {
        return false;
    }

    protected void assertResults(
        ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        Logger logger
    ) {
        var actualColumnNames = actualColumns.stream().map(c -> c.get("name")).toList();
        var actualColumnTypes = actualColumns.stream().map(c -> CsvTestUtils.Type.asType(c.get("type"))).toList();
        assertMetadata(expected, actualColumnNames, actualColumnTypes, logger);
        assertDataWithValueConverter(
            expected,
            actualValues,
            testCase.ignoreOrder,
            ignoreValueOrder(),
            enableRoundingDoubleValuesOnAsserting(),
            logger
        );
    }

    /**
     * Rounds double values when asserting double values returned in queries.
     * By default, no rounding is performed.
     */
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return false;
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @After
    public void assertRequestBreakerEmptyAfterTests() throws Exception {
        if (testClustersOk) {
            assertRequestBreakerEmpty();
        }
    }

    public static void assertRequestBreakerEmpty() throws Exception {
        assertBusy(() -> {
            HttpEntity entity = adminClient().performRequest(new Request("GET", "/_nodes/stats?metric=breaker")).getEntity();
            Map<?, ?> stats = XContentHelper.convertToMap(XContentType.JSON.xContent(), entity.getContent(), false);
            Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");

            MapMatcher breakersEmpty = matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b");

            MapMatcher nodesMatcher = matchesMap();
            for (Object name : nodes.keySet()) {
                nodesMatcher = nodesMatcher.entry(
                    name,
                    matchesMap().extraOk().entry("breakers", matchesMap().extraOk().entry("request", breakersEmpty))
                );
            }
            assertMap("circuit breakers not reset to 0", stats, matchesMap().extraOk().entry("nodes", nodesMatcher));
        });
    }

    protected boolean supportsTook() {
        if (supportsTook == null) {
            supportsTook = hasCapabilities(adminClient(), List.of("usage_contains_took"));
        }
        return supportsTook;
    }

    protected boolean supportsViews() {
        if (supportsViews == null) {
            try {
                // Keep the views support probe identical to the data loader to avoid drift across test setup paths.
                supportsViews = CsvTestsDataLoader.clusterSupportsViews(adminClient());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return supportsViews;
    }

    private String tookKey(long took) {
        if (took < 10) {
            return "lt_10ms";
        }
        if (took < 100) {
            return "lt_100ms";
        }
        if (took < TookMetrics.ONE_SECOND) {
            return "lt_1s";
        }
        if (took < TookMetrics.TEN_SECONDS) {
            return "lt_10s";
        }
        if (took < TookMetrics.ONE_MINUTE) {
            return "lt_1m";
        }
        if (took < TookMetrics.TEN_MINUTES) {
            return "lt_10m";
        }
        if (took < TookMetrics.ONE_DAY) {
            return "lt_1d";
        }
        return "gt_1d";
    }
}
