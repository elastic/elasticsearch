/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class ComputeServiceReductionTests extends ESTestCase {

    public void testRemoteFetchHandleUsesSyntheticAttributeName() {
        assertThat(RemoteFetchHandle.ATTRIBUTE_NAME, startsWith(Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX));
    }

    public void testReductionConsumesOptimizerRemoteFetchBoundary() {
        Configuration configuration = remoteFetchConfiguration();
        PhysicalPlan distributedPlan = distributedQueryPlan(configuration);
        var split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedPlan, configuration);
        assertThat(split.v1().collect(RemoteFetchExec.class), hasSize(1));
        ExchangeSinkExec dataPlan = as(split.v2(), ExchangeSinkExec.class);
        RemoteFetchBoundaryExec boundary = as(dataPlan.child(), RemoteFetchBoundaryExec.class);

        assertThat(boundary.eagerAttributes().stream().map(Attribute::name).toList(), equalTo(List.of("hire_date")));
        assertThat(
            split.v1().collect(RemoteFetchExec.class).getFirst().attributesToFetch().stream().map(Attribute::name).toList(),
            containsInAnyOrder("salary", "emp_no")
        );

        ReductionPlan reductionPlan = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            FoldContext.small(),
            dataPlan,
            true,
            true,
            "node-a",
            "session-a[n]",
            null
        );

        assertThat(reductionPlan.dataNodePlan().output(), equalTo(boundary.dataOutput()));
        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(boundary.handoffOutput()));
        assertThat(reductionPlan.dataNodePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
        assertThat(reductionPlan.nodeReducePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
        ProjectExec handleProject = as(reductionPlan.nodeReducePlan().child(), ProjectExec.class);
        EvalExec handleEval = as(handleProject.child(), EvalExec.class);
        TopNExec reductionTopN = handleEval.child().collect(TopNExec.class).getFirst();
        assertThat(reductionTopN.inputOrdering(), equalTo(TopNOperator.InputOrdering.SORTED));
        Alias handleAlias = handleEval.fields().getFirst();
        assertThat(handleAlias.toAttribute().id(), equalTo(boundary.handleAttribute().id()));
        RemoteFetchHandleFunction handleFunction = as(handleAlias.child(), RemoteFetchHandleFunction.class);
        assertThat(handleFunction.dataType(), equalTo(DataType.KEYWORD));
        assertThat(
            handleFunction,
            equalTo(new RemoteFetchHandleFunction(Source.EMPTY, boundary.documentAttribute(), "node-a", "session-a[n]"))
        );
    }

    public void testRemoteFetchBoundaryRequiresRetainedSearchContextsAtRequestHandling() {
        Configuration configuration = remoteFetchConfiguration();
        ExchangeSinkExec dataPlan = as(
            PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedQueryPlan(configuration), configuration).v2(),
            ExchangeSinkExec.class
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateRemoteFetchRequest(dataPlan, false, TransportVersion.current())
        );
        assertThat(e.getMessage(), containsString("requires retained search contexts"));
    }

    public void testRemoteFetchBoundaryRequiresFeatureTransportVersionAtRequestHandling() {
        Configuration configuration = remoteFetchConfiguration();
        ExchangeSinkExec dataPlan = as(
            PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedQueryPlan(configuration), configuration).v2(),
            ExchangeSinkExec.class
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateRemoteFetchRequest(
                dataPlan,
                true,
                TransportVersionUtils.getPreviousVersion(RemoteFetchBoundaryExec.ESQL_REMOTE_FETCH_TOPN_REDUCTION)
            )
        );
        assertThat(e.getMessage(), containsString("requires transport version"));
    }

    public void testReductionRejectsRemoteFetchHandleWithoutBoundary() {
        Attribute handle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
        ExchangeSinkExec sink = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(handle),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(handle), false)
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                sink,
                true,
                true,
                null
            )
        );
        assertThat(e.getMessage(), containsString("remote-fetch handle without a boundary"));
    }

    public void testUserColumnNamedLikeRemoteFetchHandleUsesOrdinaryReductionApi() {
        Attribute userColumn = new ReferenceAttribute(Source.EMPTY, null, RemoteFetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        ExchangeSinkExec sink = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(userColumn),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(userColumn), false)
        );

        assertFalse(RemoteFetchHandle.isRemoteFetchHandleCarrier(userColumn));
        ReductionPlan reduction = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            sink,
            false,
            false,
            null
        );
        assertThat(reduction.dataNodePlan(), equalTo(sink));
        assertThat(reduction.nodeReducePlan().output(), equalTo(sink.output()));
    }

    public void testReductionRejectsRemoteFetchBoundaryWithoutDirectFragment() {
        Configuration configuration = remoteFetchConfiguration();
        ExchangeSinkExec dataPlan = as(
            PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedQueryPlan(configuration), configuration).v2(),
            ExchangeSinkExec.class
        );
        RemoteFetchBoundaryExec boundary = as(dataPlan.child(), RemoteFetchBoundaryExec.class);
        RemoteFetchBoundaryExec malformedBoundary = new RemoteFetchBoundaryExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, boundary.dataOutput(), false),
            boundary.documentAttribute(),
            boundary.handleAttribute(),
            boundary.eagerAttributes()
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> reduceRemoteFetch(configuration, dataPlan.replaceChild(malformedBoundary))
        );
        assertThat(e.getMessage(), containsString("expected direct Fragment child"));
        assertThat(e.getMessage(), not(containsString(malformedBoundary.toString())));
    }

    public void testReductionRejectsNestedPipelineBreakerBelowRemoteFetchTopN() {
        Configuration configuration = remoteFetchConfiguration();
        ExchangeSinkExec dataPlan = as(
            PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedQueryPlan(configuration), configuration).v2(),
            ExchangeSinkExec.class
        );
        RemoteFetchBoundaryExec boundary = as(dataPlan.child(), RemoteFetchBoundaryExec.class);
        FragmentExec fragment = as(boundary.child(), FragmentExec.class);
        Project project = as(fragment.fragment(), Project.class);
        TopN topN = as(project.child(), TopN.class);
        Project nestedProject = project.replaceChild(topN.replaceChild(topN));
        RemoteFetchBoundaryExec malformedBoundary = boundary.replaceChild(fragment.withFragment(nestedProject));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> reduceRemoteFetch(configuration, dataPlan.replaceChild(malformedBoundary))
        );
        assertThat(e.getMessage(), containsString("nested pipeline breaker"));
        assertThat(e.getMessage(), not(containsString(malformedBoundary.toString())));
    }

    public void testRemoteFetchBoundaryTakesPrecedenceWhenNodeLevelReductionIsDisabled() {
        Configuration configuration = remoteFetchConfiguration();
        ReductionPlan reduction = reduceRemoteFetch(configuration, remoteFetchDataPlan(configuration), false, true, null);

        assertThat(reduction.nodeReducePlan().collect(TopNExec.class), hasSize(1));
        assertThat(reduction.nodeReducePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
        assertThat(reduction.dataNodePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testRemoteFetchBoundaryTakesPrecedenceWhenBothOrdinaryReductionFlagsAreDisabled() {
        Configuration configuration = remoteFetchConfiguration();
        ReductionPlan reduction = reduceRemoteFetch(configuration, remoteFetchDataPlan(configuration), false, false, null);

        assertThat(reduction.nodeReducePlan().collect(TopNExec.class), hasSize(1));
        assertThat(reduction.nodeReducePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
        assertThat(reduction.dataNodePlan().collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testRemoteFetchBoundaryReductionRecordsProfilingTime() {
        Configuration configuration = remoteFetchConfiguration();
        PlanTimeProfile profile = new PlanTimeProfile();

        reduceRemoteFetch(configuration, remoteFetchDataPlan(configuration), false, false, profile);

        assertThat(profile, not(equalTo(new PlanTimeProfile())));
    }

    private static PhysicalPlan distributedQueryPlan(Configuration configuration) {
        Map<String, EsField> mapping = Map.of(
            "hire_date",
            new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "salary",
            new EsField("salary", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "emp_no",
            new EsField("emp_no", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Analyzer analyzer = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping, Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(TransportVersion.current())
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no"
        );
    }

    private static Configuration remoteFetchConfiguration() {
        return EsqlTestUtils.configuration(new QueryPragmas(Settings.builder().put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), true).build()));
    }

    private static ExchangeSinkExec remoteFetchDataPlan(Configuration configuration) {
        return as(
            PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedQueryPlan(configuration), configuration).v2(),
            ExchangeSinkExec.class
        );
    }

    private static ReductionPlan reduceRemoteFetch(Configuration configuration, ExchangeSinkExec dataPlan) {
        return reduceRemoteFetch(configuration, dataPlan, true, true, null);
    }

    private static ReductionPlan reduceRemoteFetch(
        Configuration configuration,
        ExchangeSinkExec dataPlan,
        boolean runNodeLevelReduction,
        boolean reduceNodeLateMaterialization,
        PlanTimeProfile profile
    ) {
        return ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            FoldContext.small(),
            dataPlan,
            runNodeLevelReduction,
            reduceNodeLateMaterialization,
            "node-a",
            "session-a[n]",
            profile
        );
    }

    private static <T> T as(Object value, Class<T> expectedType) {
        assertThat(value, instanceOf(expectedType));
        return expectedType.cast(value);
    }
}
