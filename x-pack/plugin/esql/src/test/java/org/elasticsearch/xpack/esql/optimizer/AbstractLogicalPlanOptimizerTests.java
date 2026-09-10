/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Every test in this hierarchy runs twice. {@code current} pins {@link TransportVersion#current()} on both the analyzer and
 * the optimizer so a version-gated plan change is exercised in its own PR; {@code historical} draws one random compatible
 * version per test instance. Analyzer and optimizer of one instance share that version, as a real coordinator's do, unless an
 * analyzer needs a feature floor above it ({@link #minimumVersionAtLeast}) or a test optimizes with
 * {@link #logicalOptimizerWithLatestVersion}. Before, the optimizer drew once per class and every analyzer drew on its own.
 */
public abstract class AbstractLogicalPlanOptimizerTests extends ESTestCase {

    /** The two runs of each test; {@link #toString()} is the parameter name shown in the test name. */
    public enum VersionMode {
        CURRENT(TransportVersion::current),
        HISTORICAL(EsqlTestUtils::randomMinimumVersion);

        private final Supplier<TransportVersion> version;

        VersionMode(Supplier<TransportVersion> version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Inherited by every subclass, which only needs a constructor passing the mode up. */
    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> params() {
        return List.of(new Object[] { VersionMode.CURRENT }, new Object[] { VersionMode.HISTORICAL });
    }

    protected final VersionMode versionMode;
    protected final TransportVersion minimumVersion;
    protected final LogicalOptimizerContext logicalOptimizerCtx;
    protected final LogicalPlanOptimizer logicalOptimizer;

    protected final LogicalPlanOptimizer logicalOptimizerWithLatestVersion;

    protected static Map<String, EsField> mapping;

    // Built in the constructor rather than @Before so subclass field initializers can already use the version.
    protected AbstractLogicalPlanOptimizerTests(VersionMode versionMode) {
        this.versionMode = versionMode;
        minimumVersion = versionMode.version.get();
        logicalOptimizerCtx = new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), minimumVersion);
        logicalOptimizer = new LogicalPlanOptimizer(logicalOptimizerCtx);
        logicalOptimizerWithLatestVersion = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(logicalOptimizerCtx.configuration(), logicalOptimizerCtx.foldCtx(), TransportVersion.current())
        );
    }

    public static class TestSubstitutionOnlyOptimizer extends LogicalPlanOptimizer {
        // A static instance of this would break the EsqlNodeSubclassTests because its initialization requires a Random instance.

        public TestSubstitutionOnlyOptimizer() {
            super(unboundLogicalOptimizerContext());
        }

        public TestSubstitutionOnlyOptimizer(TransportVersion minimumVersion) {
            super(unboundLogicalOptimizerContext(minimumVersion));
        }

        public TestSubstitutionOnlyOptimizer(LogicalOptimizerContext context) {
            super(context);
        }

        @Override
        protected List<Batch<LogicalPlan>> batches() {
            return List.of(substitutions());
        }
    }

    @BeforeClass
    public static void initMapping() {
        mapping = loadMapping("mapping-basic.json");
    }

    public void testAnalyzerAndOptimizerShareModeVersion() {
        assertThat(defaultAnalyzer().buildContext().minimumVersion(), equalTo(logicalOptimizerCtx.minimumVersion()));
        assertTrue(metricsAnalyzer().buildContext().minimumVersion().supports(DimensionValues.DIMENSION_VALUES_VERSION));
        if (versionMode == VersionMode.CURRENT) {
            assertThat(logicalOptimizerCtx.minimumVersion(), equalTo(TransportVersion.current()));
        }
    }

    /** An analyzer at this instance's version; shadows {@link EsqlTestUtils#analyzer()} so every helper below shares it. */
    protected TestAnalyzer analyzer() {
        return EsqlTestUtils.analyzer().minimumTransportVersion(minimumVersion);
    }

    /** This instance's version, or {@code floor} when it does not support it, for analyzers that need a feature the floor gates. */
    protected TransportVersion minimumVersionAtLeast(TransportVersion floor) {
        assert floor.supports(PackDimsAgg.PACK_DIMS_AGG_VERSION) == false
            : "packsDimsInAggregate() assumes every floor predates pack_dims_agg";
        return minimumVersion.supports(floor) ? minimumVersion : floor;
    }

    private static TestAnalyzer addEnrichPolicies(TestAnalyzer analyzer) {
        return analyzer.addEnrichPolicy(MATCH_TYPE, "languages_idx", "id", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages_remote", "id", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.COORDINATOR, MATCH_TYPE, "languages_coordinator", "id", "languages_idx", "mapping-languages.json");
    }

    protected TestAnalyzer analyzerWithEnrichPolicies() {
        return addEnrichPolicies(analyzer());
    }

    protected TestAnalyzer defaultAnalyzer() {
        return analyzerWithEnrichPolicies().addEmployees("test").addEmployees().addLanguagesLookup().addTestLookup().addSpatialLookup();
    }

    protected TestAnalyzer airportsAnalyzer() {
        return analyzerWithEnrichPolicies().addAirports().addLanguagesLookup().addTestLookup().addSpatialLookup();
    }

    protected TestAnalyzer typesAnalyzer() {
        return analyzerWithEnrichPolicies().addAnalysisTestsInferenceResolution().addIndex("types", "mapping-all-types.json");
    }

    protected TestAnalyzer extraAnalyzer() {
        return analyzerWithEnrichPolicies().addIndex("extra", "mapping-extra.json");
    }

    protected TestAnalyzer metricsAnalyzer() {
        return metricsAnalyzerAt(minimumVersionAtLeast(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    /** Exact-version variant for callers outside this hierarchy, which have no mode to follow. */
    public static TestAnalyzer metricsAnalyzerAt(TransportVersion version) {
        return addEnrichPolicies(EsqlTestUtils.analyzer().minimumTransportVersion(version)).addIndex(
            "exp_histo_sample",
            "exp_histo_sample-mappings.json",
            IndexMode.TIME_SERIES
        ).addIndex("tdigest_timeseries_index", "tdigest_timeseries_index-mappings.json", IndexMode.TIME_SERIES).addK8s();
    }

    protected TestAnalyzer multiIndexAnalyzer() {
        var multiIndexMapping = loadMapping("mapping-basic.json");
        EsField partialTypeKeyword = new EsField("partial_type_keyword", KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
        multiIndexMapping.put(
            "partial_type_keyword",
            IndexResolver.wrapPartiallyUnmappedField(partialTypeKeyword, "partial_type_keyword", "partial_type_keyword", Set.of("test1"))
        );
        var multiIndex = new EsIndex(
            "multi_index",
            multiIndexMapping,
            Map.of("test1", new IndexProperties(IndexMode.STANDARD, 0), "test2", new IndexProperties(IndexMode.STANDARD, 0)),
            Map.of(),
            Map.of()
        );
        return analyzerWithEnrichPolicies().addIndex(multiIndex);
    }

    protected TestAnalyzer unionIndexAnalyzer() {
        var typesToIndices_languages = new LinkedHashMap<String, Set<String>>();
        typesToIndices_languages.put("byte", Set.of("union_types_index"));
        typesToIndices_languages.put("integer", Set.of("union_types_index_incompatible"));
        EsField languages = new InvalidMappedField("languages", typesToIndices_languages);

        var typesToIndices_lastName = new LinkedHashMap<String, Set<String>>();
        typesToIndices_lastName.put("text", Set.of("union_types_index"));
        typesToIndices_lastName.put("keyword", Set.of("union_types_index_incompatible"));
        EsField lastName = new InvalidMappedField("last_name", typesToIndices_lastName);

        var typesToIndices_salaryChange = new LinkedHashMap<String, Set<String>>();
        typesToIndices_salaryChange.put("float", Set.of("union_types_index"));
        typesToIndices_salaryChange.put("double", Set.of("union_types_index_incompatible"));
        EsField salaryChange = new InvalidMappedField("salary_change", typesToIndices_salaryChange);

        var typesToIndices_firstName = new LinkedHashMap<String, Set<String>>();
        typesToIndices_firstName.put("text", Set.of("union_types_index"));
        typesToIndices_firstName.put("keyword", Set.of("union_types_index_incompatible"));
        EsField firstName = new InvalidMappedField("first_name", typesToIndices_firstName);

        EsField idField = new EsField("id", KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
        var unionIndex = new EsIndex(
            "union_types_index*",
            Map.of("languages", languages, "last_name", lastName, "salary_change", salaryChange, "first_name", firstName, "id", idField),
            Map.of(
                "union_types_index",
                new IndexProperties(IndexMode.STANDARD, 0),
                "union_types_index_incompatible",
                new IndexProperties(IndexMode.STANDARD, 0)
            ),
            Map.of("", List.of("union_types_index*")),
            Map.of("", List.of("union_types_index_incompatible", "union_types_index"))
        );
        return analyzerWithEnrichPolicies().addAnalysisTestsInferenceResolution()
            .addIndex(unionIndex)
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected TestAnalyzer sampleDataAnalyzer() {
        return analyzerWithEnrichPolicies().addSampleData();
    }

    protected TestAnalyzer subqueryAnalyzer() {
        return analyzerWithEnrichPolicies().addEmployees("test")
            .addLanguages()
            .addSampleData()
            .addDefaultIncompatible()
            .addIndex("colors", "mapping-colors.json")
            .addK8sDownsampled()
            .addRemoteMissingIndex()
            .addEmptyIndex()
            .addNoFieldsIndex()
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected TestAnalyzer baseConversionAnalyzer() {
        return analyzerWithEnrichPolicies().addIndex("base_conversion", "mapping-base_conversion.json")
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected LogicalPlan optimize(LogicalPlan plan) {
        return logicalOptimizer.optimize(plan);
    }

    protected LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    protected LogicalPlan optimizedPlan(String query, TransportVersion transportVersion) {
        return optimize(defaultAnalyzer().minimumTransportVersion(transportVersion).buildAnalyzer().analyze(TEST_PARSER.parseQuery(query)));
    }

    protected LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    protected LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        return optimizer.optimize(defaultAnalyzer().query(query));
    }

    /**
     * Plans a {@code query} that references an external dataset via {@code FROM <datasetName>}, mirroring the
     * production path: an in-memory {@link ProjectMetadata} registers {@code datasetName} against {@code resource}
     * so {@link DatasetRewriter} rewrites the {@code FROM} target into an (unresolved) external relation, then the
     * analyzer resolves it using the given pre-resolved {@code schema} (see {@code GoldenTestCase#datasetMetadata}
     * for the golden-test equivalent of this same pattern).
     */
    protected LogicalPlan datasetPlan(String query, String datasetName, String resource, List<Attribute> schema) {
        assumeTrue("requires FROM <dataset> capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        String dataSourceName = datasetName + "_ds";
        ProjectMetadata datasetMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(
                DataSourceMetadata.TYPE,
                new DataSourceMetadata(Map.of(dataSourceName, new DataSource(dataSourceName, "test", null, Map.of())))
            )
            .datasets(Map.of(datasetName, new Dataset(datasetName, new DataSourceReference(dataSourceName), resource, null, Map.of())))
            .build();
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            TEST_PARSER.parseQuery(query),
            datasetMetadata,
            TestIndexNameExpressionResolver.newInstance()
        );
        return optimize(analyzer().externalSourceResolution(resource, schema, FileList.UNRESOLVED).buildAnalyzer().analyze(rewritten));
    }

    /**
     * From {@code pack_dims_agg} on, a {@link PackDimsAgg} inside the time-series aggregate packs its dimensions; before, a
     * {@link PackDims} node above it does. The analyzer decides this at its own version, which equals the instance version
     * for this purpose because every floor passed to {@link #minimumVersionAtLeast} predates {@code pack_dims_agg}.
     */
    protected boolean packsDimsInAggregate() {
        return minimumVersion.supports(PackDimsAgg.PACK_DIMS_AGG_VERSION);
    }

    /** The time-series aggregate whose {@code dimCount} dimensions {@code plan} packs, in the shape this instance's version produces. */
    protected TimeSeriesAggregate packedTimeSeriesAggregate(LogicalPlan plan, int dimCount) {
        if (packsDimsInAggregate()) {
            TimeSeriesAggregate aggregate = as(plan, TimeSeriesAggregate.class);
            assertThat(packedDims(aggregate.aggregates()), hasSize(dimCount));
            return aggregate;
        }
        PackDims pack = as(plan, PackDims.class);
        TimeSeriesAggregate aggregate = as(pack.child(), TimeSeriesAggregate.class);
        assertThat(pack.dims(), hasSize(dimCount));
        // PackDims references the DIMENSIONVALUES aliases by id; names differ when the grouping is aliased (BY p = pod).
        var dimensionValueIds = aggregate.aggregates()
            .stream()
            .filter(aggregation -> Alias.unwrap(aggregation) instanceof DimensionValues)
            .map(NamedExpression::id)
            .toList();
        assertThat(pack.dims().stream().map(Attribute::id).toList(), equalTo(dimensionValueIds));
        return aggregate;
    }

    /** The dimensions {@code aggregates} carry, whether one {@link DimensionValues} each or a single {@link PackDimsAgg}. */
    protected static List<Expression> packedDims(List<? extends NamedExpression> aggregates) {
        List<Expression> dims = new ArrayList<>();
        for (NamedExpression aggregate : aggregates) {
            aggregate.forEachDown(DimensionValues.class, values -> dims.add(values.field()));
            aggregate.forEachDown(PackDimsAgg.class, packed -> dims.addAll(packed.dims()));
        }
        return dims;
    }

    protected LogicalPlan planAirports(String query) {
        return optimize(airportsAnalyzer().query(query));
    }

    protected LogicalPlan planExtra(String query) {
        return optimize(extraAnalyzer().query(query));
    }

    protected LogicalPlan planTypes(String query) {
        return optimize(typesAnalyzer().query(query));
    }

    protected LogicalPlan planMetrics(String query) {
        return logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer().query(query));
    }

    protected LogicalPlan planMultiIndex(String query) {
        return optimize(multiIndexAnalyzer().query(query));
    }

    protected LogicalPlan planUnionIndex(String query) {
        return optimize(unionIndexAnalyzer().query(query));
    }

    protected LogicalPlan planSample(String query) {
        return optimize(sampleDataAnalyzer().query(query));
    }

    protected LogicalPlan planSubquery(String query) {
        return optimize(subqueryAnalyzer().query(query));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    protected <T extends Throwable> void failPlan(String esql, Class<T> exceptionClass, String reason) {
        var e = expectThrows(exceptionClass, () -> plan(esql));
        assertThat(e.getMessage(), containsString(reason));
    }

    protected void failPlan(String esql, String reason) {
        failPlan(esql, VerificationException.class, reason);
    }

}
