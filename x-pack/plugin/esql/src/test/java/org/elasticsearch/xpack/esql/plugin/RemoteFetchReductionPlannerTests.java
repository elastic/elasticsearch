/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteFetchReductionPlannerTests extends ESTestCase {
    public void testDistributedPlannerOwnsCoordinatorRewrite() {
        Configuration configuration = EsqlTestUtils.configuration(
            new QueryPragmas(Settings.builder().put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), true).build())
        );
        PhysicalPlan physicalPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            configuration
        );

        var planned = DistributedPlanPlanner.plan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            FoldContext.small(),
            physicalPlan,
            Map.of(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(new String[] { "employees" }, SearchRequest.DEFAULT_INDICES_OPTIONS)),
            TransportVersion.current()
        );

        assertThat(planned.coordinatorPlan().collect(RemoteFetchExec.class), hasSize(1));
        assertTrue(planned.hasConcreteIndices());
        assertTrue(planned.retainSearchContexts());
    }

    public void testPlansCoordinatorTopNFromQueryText() {
        RemoteFetchReductionPlanner.CoordinatorPlan planned = planQuery(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no"
        ).orElseThrow();

        RemoteFetchExec remoteFetch = planned.coordinatorPlan()
            .collect(RemoteFetchExec.class::isInstance)
            .stream()
            .map(RemoteFetchExec.class::cast)
            .findFirst()
            .orElseThrow();
        assertThat(remoteFetch.attributesToFetch().stream().map(Attribute::name).toList(), containsInAnyOrder("salary", "emp_no"));
        assertThat(
            planned.dataNodePlan().output().stream().map(Attribute::name).toList(),
            equalTo(List.of(RemoteFetchHandle.ATTRIBUTE_NAME, "hire_date"))
        );
    }

    public void testDoesNotPlanAggregationFromQueryText() {
        assertTrue(planQuery("FROM employees | STATS total = SUM(salary) | SORT total DESC | LIMIT 5").isEmpty());
    }

    public void testDoesNotPlanExpressionBeforeTopNFromQueryText() {
        assertTrue(planQuery("FROM employees | EVAL x = salary + 1 | SORT hire_date | LIMIT 20 | KEEP hire_date, x").isEmpty());
    }

    public void testDoesNotPlanFieldsWithSpecializedLoaderSemantics() {
        assertSpecializedFieldIsNotFetchable(new PotentiallyUnmappedKeywordEsField("specialized"));
        assertSpecializedFieldIsNotFetchable(
            new MultiTypeEsField("specialized", DataType.DATE_NANOS, true, Map.of(), EsField.TimeSeriesFieldType.NONE, null)
        );
    }

    public void testCoordinatorAndReducePlansUseRemoteFetchHandleSchema() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute hireDate = field("hire_date", DataType.DATETIME);
        Attribute salary = field("salary", DataType.INTEGER);
        Attribute empNo = field("emp_no", DataType.INTEGER);
        List<Order> order = List.of(new Order(Source.EMPTY, hireDate, Order.OrderDirection.ASC, Order.NullsPosition.LAST));

        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "employees",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("employees", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of(doc, hireDate, salary, empNo)
        );
        Project fieldsNeededBeforeTopN = new Project(Source.EMPTY, relation, List.of(doc, hireDate));
        TopN topN = new TopN(Source.EMPTY, fieldsNeededBeforeTopN, order, EsqlTestUtils.of(20), false);
        Project finalFields = new Project(Source.EMPTY, topN, List.of(hireDate, salary, empNo));

        /*
         * coordinator: Project[hire_date, salary, emp_no]
         *                  \- TopN[hire_date]
         *                       \- ExchangeSource[hire_date, salary, emp_no]
         * data: ExchangeSink[hire_date, salary, emp_no]
         *           \- Fragment[Project[hire_date, salary, emp_no] -> TopN[hire_date] -> EsRelation]
         */
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(Source.EMPTY, finalFields.output(), false, new FragmentExec(finalFields));
        PhysicalPlan coordinatorPlan = new ProjectExec(
            Source.EMPTY,
            new TopNExec(Source.EMPTY, new ExchangeSourceExec(Source.EMPTY, dataNodePlan.output(), false), order, EsqlTestUtils.of(20), 0),
            finalFields.output()
        );

        RemoteFetchReductionPlanner.CoordinatorPlan planned = RemoteFetchReductionPlanner.planCoordinatorTopN(
            contextFactory(),
            dataNodePlan,
            coordinatorPlan
        ).orElseThrow();

        /*
         * coordinator: Project[hire_date, salary, emp_no]
         *                  \- RemoteFetch[salary, emp_no]
         *                       |- TopN[hire_date] -> ExchangeSource[handle, hire_date]
         *                       \- Fragment[RemoteFetchSource[salary, emp_no]]
         * data: ExchangeSink[handle, hire_date]
         *           \- Fragment[Project[doc, hire_date] -> TopN[hire_date] -> EsRelation]
         */
        assertThat(planned.dataNodePlan().output().getFirst().name(), equalTo(RemoteFetchHandle.ATTRIBUTE_NAME));
        assertThat(planned.dataNodePlan().output(), equalTo(List.of(planned.dataNodePlan().output().getFirst(), hireDate)));
        assertThat(planned.dataNodePlan().child().output(), equalTo(List.of(doc, hireDate)));

        ProjectExec rewrittenProject = as(planned.coordinatorPlan(), ProjectExec.class);
        RemoteFetchExec remoteFetch = as(rewrittenProject.child(), RemoteFetchExec.class);
        assertThat(remoteFetch.attributesToFetch(), equalTo(List.of(salary, empNo)));
        assertThat(remoteFetch.fetchedOutputAttributes(), equalTo(List.of(salary, empNo)));
        assertThat(remoteFetch.child(), instanceOf(TopNExec.class));

        ReductionPlan reductionPlan = ReductionPlanner.plan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            planned.dataNodePlan(),
            true,
            true,
            new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]"),
            null
        );

        /*
         * shard data: ExchangeSink[doc, hire_date]
         *                 \- Fragment[Project[doc, hire_date] -> TopN[hire_date] -> EsRelation]
         * node reduce: ExchangeSink[handle, hire_date]
         *                  \- Project[handle, hire_date]
         *                       \- Eval[handle] -> TopN[hire_date] -> ExchangeSource[doc, hire_date]
         */
        assertThat(reductionPlan.dataNodePlan().output(), equalTo(List.of(doc, hireDate)));
        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(planned.dataNodePlan().output()));

        ProjectExec handleProject = as(reductionPlan.nodeReducePlan().child(), ProjectExec.class);
        EvalExec handleEval = as(handleProject.child(), EvalExec.class);
        Alias handleAlias = handleEval.fields().getFirst();
        Attribute plannedHandle = planned.dataNodePlan().output().getFirst();
        assertTrue(plannedHandle.synthetic());
        assertThat(handleAlias.toAttribute().name(), equalTo(plannedHandle.name()));
        assertThat(handleAlias.toAttribute().dataType(), equalTo(plannedHandle.dataType()));
        assertThat(handleAlias.toAttribute().id(), equalTo(plannedHandle.id()));
        assertThat(handleAlias.child(), instanceOf(RemoteFetchHandleFunction.class));
    }

    public void testUserColumnNamedLikeRemoteFetchHandleIsNotTreatedAsInternalHandle() {
        Attribute userColumn = new ReferenceAttribute(Source.EMPTY, null, RemoteFetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(userColumn),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(userColumn), false)
        );

        // A non-synthetic user column with the reserved name must not satisfy the internal handle contract.
        assertFalse(RemoteFetchHandle.isAttribute(userColumn));
        assertTrue(
            RemoteFetchReductionPlanner.planReduceDriverTopN(
                contextFactory(),
                plan,
                new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]")
            ).isEmpty()
        );
    }

    public void testFailsWhenCoordinatorCommittedButReductionCannotBeRebuilt() {
        Attribute handle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
        Attribute sort = field("sort", DataType.LONG);
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of(sort)
        );
        // The fragment lacks the top-level Project that planReduceDriverTopN requires, so the remote-fetch rewrite declines.
        TopN topN = new TopN(Source.EMPTY, relation, order, EsqlTestUtils.of(10), false);
        ExchangeSinkExec sink = new ExchangeSinkExec(Source.EMPTY, List.of(handle, sort), false, new FragmentExec(topN));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ReductionPlanner.plan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                sink,
                true,
                true,
                new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]"),
                null
            )
        );
        assertThat(e.getMessage(), containsString("node reduction could not be rebuilt"));
    }

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static void assertSpecializedFieldIsNotFetchable(EsField specializedField) {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute sort = field("sort", DataType.LONG);
        Attribute specialized = new FieldAttribute(Source.EMPTY, "specialized", specializedField);
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of(doc, sort, specialized)
        );
        Project dataProject = new Project(
            Source.EMPTY,
            new TopN(Source.EMPTY, new Project(Source.EMPTY, relation, List.of(doc, sort)), order, EsqlTestUtils.of(10), false),
            List.of(sort, specialized)
        );
        ExchangeSinkExec dataPlan = new ExchangeSinkExec(Source.EMPTY, dataProject.output(), false, new FragmentExec(dataProject));
        PhysicalPlan coordinatorPlan = new ProjectExec(
            Source.EMPTY,
            new TopNExec(Source.EMPTY, new ExchangeSourceExec(Source.EMPTY, dataPlan.output(), false), order, EsqlTestUtils.of(10), 0),
            dataProject.output()
        );

        assertTrue(RemoteFetchReductionPlanner.planCoordinatorTopN(contextFactory(), dataPlan, coordinatorPlan).isEmpty());
    }

    private static Optional<RemoteFetchReductionPlanner.CoordinatorPlan> planQuery(String query) {
        PhysicalPlan distributedPlan = distributedQueryPlan(query, EsqlTestUtils.TEST_CFG);
        var coordinatorAndDataNode = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedPlan, EsqlTestUtils.TEST_CFG);
        return RemoteFetchReductionPlanner.planCoordinatorTopN(
            contextFactory(),
            as(coordinatorAndDataNode.v2(), ExchangeSinkExec.class),
            coordinatorAndDataNode.v1()
        );
    }

    private static PhysicalPlan distributedQueryPlan(String query, Configuration configuration) {
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
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query);
    }

    private static Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory() {
        return stats -> new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            stats
        );
    }

    private static <T> T as(Object value, Class<T> expectedType) {
        assertThat(value, instanceOf(expectedType));
        return expectedType.cast(value);
    }
}
