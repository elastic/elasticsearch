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
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
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
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.SourceFanInExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewCompaction;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Production-order planning for a FORK over a source-only view composition.
 * Linked namesakes are supplied as analyzer resolutions; this is not a live
 * linked-project cluster.
 */
public class SourceFanInProductionOrderTests extends ESTestCase {

    private static final String NO_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";
    private static final IndexNameExpressionResolver RESOLVER = TestIndexNameExpressionResolver.newInstance();
    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);
    private static final int DATASET_COUNT = 7;
    private static final String DS1_PATH = datasetPath(1);
    private static final String DS2_PATH = datasetPath(2);
    private static final String VIEW = "view_fan";
    private static final String EXTRA_IDX = "extra_idx";
    private static final String SEVEN_DATASET_VIEW = "FROM ds1, ds2, ds3, ds4, ds5, ds6, ds7";
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

    public void testSingleDatasetViewsStillRejectFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            putView(viewService, "view_a", "FROM ds1");
            putView(viewService, "view_b", "FROM ds1");

            String query = """
                FROM view_a, view_b
                | FORK
                    (KEEP emp_no)
                    (WHERE emp_no IS NOT NULL | KEEP emp_no)
                """;
            VerificationException error = expectThrows(
                VerificationException.class,
                () -> analyzeProductionOrder(viewService, query, false, UnmappedResolution.DEFAULT, false)
            );
            assertThat(error.getMessage(), containsString("FORK after subquery is not supported"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testEstimateRowSizeIsolatedWhenOneForkBranchIsLocal() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2");

            LogicalPlan analyzed = analyzeProductionOrder(viewService, FORK_QUERY, false);
            PhysicalPlan physical = mapPlan(analyzed);
            MergeExec merge = onlyPhysical(physical, MergeExec.class);
            PhysicalPlan fanInBranch = null;
            for (PhysicalPlan child : merge.children()) {
                if (child.anyMatch(p -> p instanceof SourceFanInExec)) {
                    fanInBranch = child;
                    break;
                }
            }
            assertNotNull(fanInBranch);
            EvalExec localEval = new EvalExec(
                merge.source(),
                new LocalSourceExec(merge.source(), merge.output(), EmptyLocalSupplier.EMPTY),
                List.of(new Alias(merge.source(), "added", new Literal(merge.source(), 1, DataType.INTEGER)))
            );
            ProjectExec localBranch = new ProjectExec(merge.source(), localEval, merge.output());
            List<Integer> isolatedSizes = producerRowSizes(fanInOf(EstimatesRowSize.estimateRowSize(0, fanInBranch)));
            assertFalse(isolatedSizes.isEmpty());
            for (List<PhysicalPlan> branches : List.of(List.of(localBranch, fanInBranch), List.of(fanInBranch, localBranch))) {
                MergeExec mixed = new MergeExec(merge.source(), branches, merge.output());
                MergeExec estimated = as(EstimatesRowSize.estimateRowSize(0, mixed), MergeExec.class);
                SourceFanInExec estimatedFanIn = fanInOf(estimated);
                assertNotNull(estimatedFanIn);
                assertThat(producerRowSizes(estimatedFanIn), equalTo(isolatedSizes));
            }
            List<Integer> leakedSizes = producerRowSizes(
                fanInOf(EstimatesRowSize.estimateRowSize(DataType.INTEGER.estimatedSize(), fanInBranch))
            );
            assertThat(leakedSizes, not(equalTo(isolatedSizes)));
            assertThat(
                leakedSizes.stream().mapToInt(Integer::intValue).sum(),
                greaterThan(isolatedSizes.stream().mapToInt(Integer::intValue).sum())
            );
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testUnmatchedNamesakeAtEightProducersAdmitsFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, SEVEN_DATASET_VIEW);

            LogicalPlan analyzed = analyzeProductionOrder(viewService, FORK_QUERY, false);
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewUnionAll));
            List<SourceFanInUnionAll> fanIns = collectLogical(analyzed, SourceFanInUnionAll.class);
            assertThat(fanIns, hasSize(2));
            for (SourceFanInUnionAll fanIn : fanIns) {
                assertThat(fanIn.children(), hasSize(SourceFanInUnionAll.MAX_PRODUCERS));
                assertThat(countLogical(fanIn, ExternalRelation.class), equalTo(DATASET_COUNT));
                assertThat(countLogical(fanIn, EsRelation.class), equalTo(1));
            }
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testMatchedNamesakeNinthProducerRejectsFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, SEVEN_DATASET_VIEW);

            VerificationException error = expectThrows(
                VerificationException.class,
                () -> analyzeProductionOrder(viewService, FORK_QUERY, true)
            );
            assertThat(error.getMessage(), containsString("FROM supports up to " + SourceFanInUnionAll.MAX_PRODUCERS + " sources"));
            assertThat(error.getMessage(), containsString("got: 9"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testMatchedEmptyMappingNamesakeNinthProducerRejectsFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, SEVEN_DATASET_VIEW);

            IndexResolution emptyMapping = IndexResolution.valid(EsIndexGenerator.esIndex(VIEW), Set.of(VIEW), Map.of());
            VerificationException error = expectThrows(
                VerificationException.class,
                () -> analyzeProductionOrder(viewService, FORK_QUERY, emptyMapping, UnmappedResolution.DEFAULT, true)
            );
            assertThat(error.getMessage(), containsString("FROM supports up to " + SourceFanInUnionAll.MAX_PRODUCERS + " sources"));
            assertThat(error.getMessage(), containsString("got: 9"));
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

    public void testNestedSourceOnlyViewChainFlattensUnderFork() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, "view_inner", "FROM ds1, ds2");
            putView(viewService, VIEW, "FROM view_inner");

            LogicalPlan analyzed = analyzeProductionOrder(viewService, FORK_QUERY, false);
            assertFalse(analyzed.anyMatch(p -> p instanceof ViewUnionAll));
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
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testMissingColumnNullFillsAndPreservesOutputOrder() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            viewService.addIndex(ProjectId.DEFAULT, EXTRA_IDX);
            putView(viewService, VIEW, "FROM ds1, ds2");

            Map<String, List<Attribute>> schemas = Map.of(
                DS1_PATH,
                List.of(referenceAttribute("emp_no", DataType.INTEGER), referenceAttribute("dept", DataType.KEYWORD)),
                DS2_PATH,
                List.of(referenceAttribute("emp_no", DataType.INTEGER))
            );
            LogicalPlan analyzed = analyzeProductionOrder(viewService, "FROM view_fan, extra_idx | KEEP emp_no, dept", schemas);
            List<SourceFanInUnionAll> fanIns = collectLogical(analyzed, SourceFanInUnionAll.class);
            assertThat(fanIns, hasSize(1));
            SourceFanInUnionAll fanIn = fanIns.getFirst();
            assertThat(Expressions.names(fanIn.output()), equalTo(List.of("emp_no", "dept")));
            assertTrue(hasNullFill(fanIn, "dept"));
            assertFalse(hasNullFill(fanIn, "emp_no"));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testDatetimeAndDateNanosWidenOnFanIn() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            putView(viewService, VIEW, "FROM ds1, ds2");

            Map<String, List<Attribute>> schemas = Map.of(
                DS1_PATH,
                List.of(referenceAttribute("ts", DataType.DATETIME)),
                DS2_PATH,
                List.of(referenceAttribute("ts", DataType.DATE_NANOS))
            );
            LogicalPlan analyzed = analyzeProductionOrder(viewService, "FROM view_fan | KEEP ts", schemas);
            List<SourceFanInUnionAll> fanIns = collectLogical(analyzed, SourceFanInUnionAll.class);
            assertThat(fanIns, hasSize(1));
            Attribute ts = fanIns.getFirst().output().stream().filter(attr -> "ts".equals(attr.name())).findFirst().orElseThrow();
            assertThat(ts.dataType(), equalTo(DataType.DATE_NANOS));
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testIntegerAndLongEmpNoConflictOnFanIn() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            putView(viewService, VIEW, "FROM ds1, ds2");

            Map<String, List<Attribute>> schemas = Map.of(
                DS1_PATH,
                List.of(referenceAttribute("emp_no", DataType.INTEGER)),
                DS2_PATH,
                List.of(referenceAttribute("emp_no", DataType.LONG))
            );
            LogicalPlan analyzed = analyzeProductionOrder(viewService, "FROM view_fan | KEEP emp_no", schemas);
            assertThat(producerFieldTypes(analyzed, "emp_no"), equalTo(Set.of(DataType.INTEGER, DataType.LONG)));
            assertUnsupportedFanInField(analyzed, "emp_no");
            assertWarnings(NO_LIMIT_WARNING);
            assertConflictingFanInFieldError(viewService, schemas, "integer", "long");
            assertWarnings(NO_LIMIT_WARNING);
        }
    }

    public void testIntegerAndKeywordEmpNoConflictOnFanIn() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            putView(viewService, VIEW, "FROM ds1, ds2");

            Map<String, List<Attribute>> schemas = Map.of(
                DS1_PATH,
                List.of(referenceAttribute("emp_no", DataType.INTEGER)),
                DS2_PATH,
                List.of(referenceAttribute("emp_no", DataType.KEYWORD))
            );
            LogicalPlan analyzed = analyzeProductionOrder(viewService, "FROM view_fan | KEEP emp_no", schemas);
            assertThat(producerFieldTypes(analyzed, "emp_no"), equalTo(Set.of(DataType.INTEGER, DataType.KEYWORD)));
            assertUnsupportedFanInField(analyzed, "emp_no");
            assertWarnings(NO_LIMIT_WARNING);
            assertConflictingFanInFieldError(viewService, schemas, "integer", "keyword");
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
        return analyzeProductionOrder(viewService, query, matchViewNamesake, unmappedResolution, true);
    }

    private static LogicalPlan analyzeProductionOrder(
        InMemoryViewService viewService,
        String query,
        boolean matchViewNamesake,
        UnmappedResolution unmappedResolution,
        boolean crossProjectEnabled
    ) {
        IndexResolution namesake = null;
        if (matchViewNamesake) {
            namesake = IndexResolution.valid(
                EsIndexGenerator.esIndex(VIEW, LoadMapping.loadMapping("mapping-one-field.json"), Map.of(VIEW, IndexMode.STANDARD))
            );
        }
        return analyzeProductionOrder(viewService, query, namesake, unmappedResolution, crossProjectEnabled, Map.of());
    }

    private static LogicalPlan analyzeProductionOrder(
        InMemoryViewService viewService,
        String query,
        Map<String, List<Attribute>> schemaByPath
    ) {
        return analyzeProductionOrder(viewService, query, null, UnmappedResolution.DEFAULT, true, schemaByPath);
    }

    private static LogicalPlan analyzeProductionOrder(
        InMemoryViewService viewService,
        String query,
        IndexResolution viewNamesakeResolution,
        UnmappedResolution unmappedResolution,
        boolean crossProjectEnabled
    ) {
        return analyzeProductionOrder(viewService, query, viewNamesakeResolution, unmappedResolution, crossProjectEnabled, Map.of());
    }

    private static LogicalPlan analyzeProductionOrder(
        InMemoryViewService viewService,
        String query,
        IndexResolution viewNamesakeResolution,
        UnmappedResolution unmappedResolution,
        boolean crossProjectEnabled,
        Map<String, List<Attribute>> schemaByPath
    ) {
        LogicalPlan parsed = TEST_PARSER.parseQuery(query);
        LogicalPlan views = replaceViews(viewService, parsed, crossProjectEnabled);
        LogicalPlan compacted = ViewCompaction.preIndexResolution(views);
        LogicalPlan rewritten = rewriteWithCps(compacted, projectMetadata(), crossProjectEnabled);
        LogicalPlan normalized = SourceExpansionNormalizer.normalize(rewritten);
        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer().preAnalyze(normalized);
        assertCollectedFromTransformedPlan(preAnalysis, query, crossProjectEnabled);

        EsIndex extra = EsIndexGenerator.esIndex(
            EXTRA_IDX,
            LoadMapping.loadMapping("mapping-one-field.json"),
            Map.of(EXTRA_IDX, IndexMode.STANDARD)
        );
        var builder = analyzer().externalSourceResolution(externalSources(schemaByPath)).unmappedResolution(unmappedResolution);
        for (IndexPattern pattern : preAnalysis.indexes().keySet()) {
            if (EXTRA_IDX.equals(pattern.indexPattern())) {
                builder.addIndex(pattern.indexPattern(), IndexResolution.valid(extra));
            }
        }
        for (LinkedIndexPattern linked : preAnalysis.linkedIndices()) {
            boolean matchView = viewNamesakeResolution != null && VIEW.equals(linked.pattern().indexPattern());
            builder.addLenientResolution(
                linked,
                matchView ? viewNamesakeResolution : IndexResolution.empty(linked.pattern().indexPattern())
            );
        }
        return builder.buildAnalyzer().analyze(normalized);
    }

    private static void assertCollectedFromTransformedPlan(PreAnalyzer.PreAnalysis preAnalysis, String query, boolean crossProjectEnabled) {
        List<String> indexNames = new ArrayList<>();
        for (IndexPattern pattern : preAnalysis.indexes().keySet()) {
            indexNames.add(pattern.indexPattern());
        }
        if (query.contains(EXTRA_IDX)) {
            assertThat(indexNames, hasItem(EXTRA_IDX));
        } else {
            assertFalse(indexNames.contains(EXTRA_IDX));
        }
        Set<String> registeredPaths = new HashSet<>();
        for (int i = 1; i <= DATASET_COUNT; i++) {
            registeredPaths.add(datasetPath(i));
        }
        assertTrue(registeredPaths.containsAll(preAnalysis.icebergPaths()));
        if (query.contains(VIEW) || query.contains("view_a") || query.contains("ds1")) {
            assertThat(preAnalysis.icebergPaths(), hasItem(DS1_PATH));
        }
        if (query.contains(VIEW)) {
            assertThat(preAnalysis.icebergPaths(), hasItem(DS2_PATH));
        }
        for (LinkedIndexPattern linked : preAnalysis.linkedIndices()) {
            assertEquals(LinkedIndexPattern.Kind.OPTIONAL, linked.kind());
        }
        if (crossProjectEnabled && query.contains(VIEW)) {
            assertTrue(
                preAnalysis.linkedIndices()
                    .stream()
                    .anyMatch(linked -> VIEW.equals(linked.pattern().indexPattern()) && linked.kind() == LinkedIndexPattern.Kind.OPTIONAL)
            );
        }
    }

    private static PhysicalPlan mapPlan(LogicalPlan analyzed) {
        TransportVersion version = TransportVersion.current();
        LogicalPlan optimized = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), version))
            .optimize(analyzed);
        return new Mapper().map(new Versioned<>(optimized, version));
    }

    private static LogicalPlan replaceViews(InMemoryViewService viewService, LogicalPlan plan) {
        return replaceViews(viewService, plan, true);
    }

    private static LogicalPlan replaceViews(InMemoryViewService viewService, LogicalPlan plan, boolean crossProjectEnabled) {
        CrossProjectModeDecider cps = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", crossProjectEnabled).build()
        );
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
        return rewriteWithCps(parsed, project, true);
    }

    private static LogicalPlan rewriteWithCps(LogicalPlan parsed, ProjectMetadata project, boolean crossProjectEnabled) {
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
        return DatasetRewriter.rewrite(parsed, project, resolutions, crossProjectEnabled);
    }

    private static ProjectMetadata projectMetadata() {
        DataSource parent = new DataSource("s3_parent", "test", null, Map.of());
        Map<String, Dataset> datasets = new LinkedHashMap<>();
        for (int i = 1; i <= DATASET_COUNT; i++) {
            String name = "ds" + i;
            datasets.put(name, new Dataset(name, new DataSourceReference("s3_parent"), datasetPath(i), null, Map.of("format", "parquet")));
        }
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("s3_parent", parent)))
            .datasets(datasets)
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

    private static ExternalSourceResolution externalSources(Map<String, List<Attribute>> schemaByPath) {
        List<Attribute> defaultSchema = List.of(referenceAttribute("emp_no", DataType.INTEGER));
        Map<String, ExternalSourceResolution.ResolvedSource> resolved = new LinkedHashMap<>();
        for (int i = 1; i <= DATASET_COUNT; i++) {
            String path = datasetPath(i);
            resolved.put(path, resolvedSource(path, schemaByPath.getOrDefault(path, defaultSchema)));
        }
        return new ExternalSourceResolution(resolved);
    }

    private static boolean hasNullFill(LogicalPlan plan, String name) {
        Holder<Boolean> found = new Holder<>(false);
        plan.forEachDown(Eval.class, eval -> {
            for (Alias alias : eval.fields()) {
                if (name.equals(alias.name()) && alias.child() instanceof Literal literal && literal.value() == null) {
                    found.set(true);
                }
            }
        });
        return found.get();
    }

    private static String datasetPath(int n) {
        return "s3://bucket/ds" + n + "/*.parquet";
    }

    private static SourceFanInExec fanInOf(PhysicalPlan plan) {
        Holder<SourceFanInExec> found = new Holder<>();
        plan.forEachDown(SourceFanInExec.class, found::set);
        return found.get();
    }

    private static List<Integer> producerRowSizes(SourceFanInExec fanIn) {
        List<Integer> sizes = new ArrayList<>();
        for (PhysicalPlan producer : fanIn.producers()) {
            producer.forEachDown(p -> {
                if (p instanceof FragmentExec fragment) {
                    sizes.add(fragment.estimatedRowSize());
                } else if (p instanceof ExternalSourceExec external) {
                    sizes.add(external.estimatedRowSize());
                }
            });
        }
        return sizes;
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

    private static Set<DataType> producerFieldTypes(LogicalPlan plan, String name) {
        Set<DataType> types = new HashSet<>();
        for (ExternalRelation relation : collectLogical(plan, ExternalRelation.class)) {
            for (Attribute attr : relation.output()) {
                if (name.equals(attr.name())) {
                    types.add(attr.dataType());
                }
            }
        }
        return types;
    }

    private static void assertUnsupportedFanInField(LogicalPlan plan, String name) {
        List<SourceFanInUnionAll> fanIns = collectLogical(plan, SourceFanInUnionAll.class);
        assertThat(fanIns, hasSize(1));
        Attribute attr = fanIns.getFirst().output().stream().filter(a -> name.equals(a.name())).findFirst().orElseThrow();
        assertThat(as(attr, UnsupportedAttribute.class).dataType(), equalTo(DataType.UNSUPPORTED));
    }

    private static void assertConflictingFanInFieldError(
        InMemoryViewService viewService,
        Map<String, List<Attribute>> schemas,
        String leftType,
        String rightType
    ) {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzeProductionOrder(viewService, "FROM view_fan | WHERE emp_no IS NOT NULL", schemas)
        );
        assertThat(e.getMessage(), containsString("Column [emp_no] has conflicting data types in subqueries: [" + leftType));
        assertThat(e.getMessage(), containsString(rightType));
    }
}
