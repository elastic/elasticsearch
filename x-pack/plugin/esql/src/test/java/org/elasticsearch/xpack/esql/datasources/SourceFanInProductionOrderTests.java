/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SourceFanInExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewCompaction;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Production-order planning for a FORK over a source-only view composition.
 * Linked namesakes are supplied as analyzer resolutions; this is not a live
 * linked-project cluster.
 */
public class SourceFanInProductionOrderTests extends ESTestCase {

    private static final String NO_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";
    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();
    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);
    private static final String DS1_PATH = "s3://bucket/ds1/*.parquet";
    private static final String DS2_PATH = "s3://bucket/ds2/*.parquet";
    private static final String VIEW = "view_fan";
    private static final String EXTRA_IDX = "extra_idx";
    private static final String FORK_QUERY = """
        FROM view_fan, extra_idx
        | FORK
            (KEEP emp_no)
            (WHERE emp_no IS NOT NULL | KEEP emp_no)
        | KEEP _fork, emp_no
        """;

    public void testForkOverSourceOnlyViewCompositionMapsToMergeOfFanIns() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2");

            LogicalPlan analyzed = analyzeProductionOrder(viewService, FORK_QUERY, false);
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewUnionAll));
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewShadowRelation));
            List<SourceFanInUnionAll> fanIns = collectLogical(analyzed, SourceFanInUnionAll.class);
            assertThat(fanIns, hasSize(2));
            for (SourceFanInUnionAll fanIn : fanIns) {
                assertThat(fanIn.children(), hasSize(3));
                assertThat(countLogical(fanIn, ExternalRelation.class), equalTo(2));
                assertThat(countLogical(fanIn, EsRelation.class), equalTo(1));
            }

            PhysicalPlan physical = mapPlan(analyzed);
            assertThat(countPhysical(physical, MergeExec.class), equalTo(1));
            assertThat(countPhysical(physical, SourceFanInExec.class), equalTo(2));
            MergeExec merge = onlyPhysical(physical, MergeExec.class);
            assertThat(merge.children(), hasSize(2));
            for (PhysicalPlan branch : merge.children()) {
                assertTrue(branch.anyMatch(p -> p instanceof SourceFanInExec));
            }
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testMatchedViewNamesakeAddsAProducerUnderFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2");

            LogicalPlan analyzed = analyzeProductionOrder(viewService, FORK_QUERY, true);
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewUnionAll));
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewShadowRelation));
            List<SourceFanInUnionAll> fanIns = collectLogical(analyzed, SourceFanInUnionAll.class);
            assertThat(fanIns, hasSize(2));
            for (SourceFanInUnionAll fanIn : fanIns) {
                assertThat(fanIn.children(), hasSize(4));
                assertThat(countLogical(fanIn, ExternalRelation.class), equalTo(2));
                assertThat(countLogical(fanIn, EsRelation.class), equalTo(2));
            }

            PhysicalPlan physical = mapPlan(analyzed);
            assertThat(countPhysical(physical, MergeExec.class), equalTo(1));
            assertThat(countPhysical(physical, SourceFanInExec.class), equalTo(2));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testUserSubqueryCompositionStillRejectsFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2");

            String query = """
                FROM (FROM view_fan | EVAL marker = 1), extra_idx
                | FORK
                    (KEEP emp_no)
                    (WHERE emp_no IS NOT NULL | KEEP emp_no)
                """;
            VerificationException error = expectThrows(
                VerificationException.class,
                () -> analyzeProductionOrder(viewService, query, false)
            );
            assertThat(error.getMessage(), containsString("FORK after subquery is not supported"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testUnmappedLoadAndNullifyChangeFanInBesideViewPipeline() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2 | EVAL marker = 1");

            String query = "FROM view_fan, extra_idx | KEEP emp_no, does_not_exist";
            LogicalPlan load = analyzeProductionOrder(viewService, query, false, UnmappedResolution.LOAD);
            LogicalPlan nullify = analyzeProductionOrder(viewService, query, false, UnmappedResolution.NULLIFY);

            assertTrue(load.anyMatch(p -> p instanceof ViewUnionAll));
            assertTrue(nullify.anyMatch(p -> p instanceof ViewUnionAll));
            assertTrue(load.anyMatch(p -> p instanceof SourceFanInUnionAll));
            assertTrue(nullify.anyMatch(p -> p instanceof SourceFanInUnionAll));
            assertNotEquals(load.toString(), nullify.toString());
            assertTrue(hasUnmappedKeywordField(load, "does_not_exist"));
            assertTrue(hasMissingNullField(nullify, "does_not_exist"));
            assertFalse(hasUnmappedKeywordField(nullify, "does_not_exist"));
            assertFalse(hasMissingNullField(load, "does_not_exist"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testUnmappedLoadStillRejectsForkAfterViewPipelineAndFanIn() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2 | EVAL marker = 1");

            VerificationException error = expectThrows(
                VerificationException.class,
                () -> analyzeProductionOrder(viewService, FORK_QUERY, false, UnmappedResolution.LOAD)
            );
            assertThat(error.getMessage(), containsString("FORK after subquery is not supported"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    private static LogicalPlan analyzeProductionOrder(InMemoryViewService viewService, String query, boolean matchViewNamesake) {
        return analyzeProductionOrder(viewService, query, matchViewNamesake, UnmappedResolution.DEFAULT);
    }

    private static LogicalPlan analyzeProductionOrder(
        InMemoryViewService viewService,
        String query,
        boolean matchViewNamesake,
        UnmappedResolution unmappedResolution
    ) {
        LogicalPlan parsed = TEST_PARSER.parseQuery(query);
        LogicalPlan views = replaceViews(viewService, parsed);
        LogicalPlan compacted = ViewCompaction.preIndexResolution(views);
        LogicalPlan rewritten = rewriteWithCps(compacted, projectMetadata());
        LogicalPlan normalized = SourceExpansionNormalizer.normalize(rewritten);

        EsIndex extra = EsIndexGenerator.esIndex(
            EXTRA_IDX,
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of(EXTRA_IDX, IndexMode.STANDARD)
        );
        EsIndex viewNamesake = EsIndexGenerator.esIndex(
            VIEW,
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of(VIEW, IndexMode.STANDARD)
        );
        var builder = analyzer().addIndex(extra).externalSourceResolution(externalSources()).unmappedResolution(unmappedResolution);
        if (matchViewNamesake) {
            builder.addLenientResolution(viewNamesake);
        } else {
            builder.addLenientResolution(
                new LinkedIndexPattern(LinkedIndexPattern.Kind.OPTIONAL, new IndexPattern(Source.EMPTY, VIEW)),
                IndexResolution.empty(VIEW)
            );
        }
        Analyzer analyzer = builder.buildAnalyzer();
        return analyzer.analyze(normalized);
    }

    private static PhysicalPlan mapPlan(LogicalPlan analyzed) {
        TransportVersion version = TransportVersion.current();
        LogicalPlan optimized = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), version))
            .optimize(analyzed);
        return new Mapper().map(new Versioned<>(optimized, version));
    }

    private static LogicalPlan replaceViews(InMemoryViewService viewService, LogicalPlan plan) {
        CrossProjectModeDecider cps = new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build());
        ViewResolver resolver = viewService.getViewResolver(cps);
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        resolver.replaceViews(plan, null, parseView(), future);
        return future.actionGet().plan();
    }

    private static BiFunction<String, String, LogicalPlan> parseView() {
        return (query, viewName) -> TEST_PARSER.parseView(
            query,
            new QueryParams(),
            new SettingsValidationContext(false, false),
            EMPTY_INFERENCE_SETTINGS,
            viewName
        ).plan();
    }

    private static LogicalPlan rewriteWithCps(LogicalPlan parsed, ProjectMetadata project) {
        Map<UnresolvedRelation, DatasetRewriter.DatasetResolution> resolutions = new IdentityHashMap<>();
        Set<String> datasetNames = DatasetMetadata.get(project).datasets().keySet();
        parsed.forEachUp(UnresolvedRelation.class, relation -> {
            if (resolutions.containsKey(relation)) {
                return;
            }
            List<String> patterns = DatasetRewriter.patternsOf(relation);
            if (DatasetRewriter.hasRemotePattern(patterns)
                || DatasetRewriter.anyPatternCouldMatchDataset(patterns, datasetNames) == false) {
                return;
            }
            String[] raw = patterns.toArray(String[]::new);
            resolutions.put(relation, DatasetRewriter.resolve(raw, raw, project, RESOLVER));
        });
        return DatasetRewriter.rewrite(parsed, project, resolutions, true);
    }

    private static ProjectMetadata projectMetadata() {
        DataSource parent = new DataSource("s3_parent", "test", null, Map.of());
        Dataset ds1 = new Dataset("ds1", new DataSourceReference("s3_parent"), DS1_PATH, null, Map.of("format", "parquet"));
        Dataset ds2 = new Dataset("ds2", new DataSourceReference("s3_parent"), DS2_PATH, null, Map.of("format", "parquet"));
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(Map.of("ds1", ds1, "ds2", ds2))
            .put(
                IndexMetadata.builder(EXTRA_IDX)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .build(),
                false
            )
            .build();
    }

    private static void putView(InMemoryViewService viewService, String name, String query) {
        PutViewAction.Request request = new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query, null));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>();
        viewService.putView(ProjectId.DEFAULT, request, ActionListener.wrap(ignored -> latch.countDown(), e -> {
            err.set(e);
            latch.countDown();
        }));
        try {
            assertTrue("in-memory putView is synchronous", latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    private static ExternalSourceResolution externalSources() {
        List<Attribute> schema = List.of(referenceAttribute("emp_no", DataType.INTEGER));
        return new ExternalSourceResolution(Map.of(DS1_PATH, resolvedSource(DS1_PATH, schema), DS2_PATH, resolvedSource(DS2_PATH, schema)));
    }

    private static ExternalSourceResolution.ResolvedSource resolvedSource(String path, List<Attribute> schema) {
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        String file = path.replace("*.parquet", "f1.parquet");
        FileList files = GlobExpander.fileListOf(List.of(new StorageEntry(StoragePath.of(file), 100, Instant.EPOCH)), path);
        return new ExternalSourceResolution.ResolvedSource(metadata, files, Map.of());
    }

    private static <T extends LogicalPlan> List<T> collectLogical(LogicalPlan plan, Class<T> type) {
        List<T> found = new ArrayList<>();
        plan.forEachDown(type, found::add);
        return found;
    }

    private static <T extends PhysicalPlan> T onlyPhysical(PhysicalPlan plan, Class<T> type) {
        List<T> found = new ArrayList<>();
        plan.forEachDown(type, found::add);
        assertThat("expected exactly one " + type.getSimpleName() + ", got: " + plan, found, hasSize(1));
        return found.get(0);
    }

    private static int countLogical(LogicalPlan plan, Class<?> type) {
        int[] count = { 0 };
        plan.forEachDown(p -> {
            if (type.isInstance(p)) {
                count[0]++;
            }
        });
        return count[0];
    }

    private static int countPhysical(PhysicalPlan plan, Class<?> type) {
        int[] count = { 0 };
        plan.forEachDown(p -> {
            if (type.isInstance(p)) {
                count[0]++;
            }
        });
        return count[0];
    }

    private static boolean hasUnmappedKeywordField(LogicalPlan plan, String name) {
        Holder<Boolean> found = new Holder<>(false);
        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (name.equals(fa.name()) && fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                found.set(true);
            }
        });
        return found.get();
    }

    private static boolean hasMissingNullField(LogicalPlan plan, String name) {
        Holder<Boolean> found = new Holder<>(false);
        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (name.equals(fa.name()) && (fa.field() instanceof MissingEsField || fa.dataType() == DataType.NULL)) {
                found.set(true);
            }
        });
        return found.get();
    }
}
