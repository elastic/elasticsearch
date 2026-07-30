/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
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
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteFetchReductionPlannerTests extends ESTestCase {
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
            Map.of("employees", IndexMode.STANDARD),
            List.of(doc, hireDate, salary, empNo)
        );
        Project fieldsNeededBeforeTopN = new Project(Source.EMPTY, relation, List.of(doc, hireDate));
        TopN topN = new TopN(Source.EMPTY, fieldsNeededBeforeTopN, order, EsqlTestUtils.of(20), false);
        Project finalFields = new Project(Source.EMPTY, topN, List.of(hireDate, salary, empNo));
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

        assertThat(planned.dataNodePlan().output().getFirst().name(), equalTo(RemoteFetchReductionPlanner.HANDLE_ATTRIBUTE_NAME));
        assertThat(planned.dataNodePlan().output(), equalTo(List.of(planned.dataNodePlan().output().getFirst(), hireDate)));
        assertThat(planned.dataNodePlan().child().output(), equalTo(List.of(doc, hireDate)));

        ProjectExec rewrittenProject = as(planned.coordinatorPlan(), ProjectExec.class);
        RemoteFetchExec remoteFetch = as(rewrittenProject.child(), RemoteFetchExec.class);
        assertThat(remoteFetch.attributesToFetch(), equalTo(List.of(salary, empNo)));
        assertThat(remoteFetch.fetchedOutputAttributes(), equalTo(List.of(salary, empNo)));
        assertThat(remoteFetch.child(), instanceOf(TopNExec.class));

        ReductionPlan reductionPlan = RemoteFetchReductionPlanner.planReduceDriverTopN(
            contextFactory(),
            planned.dataNodePlan(),
            "node-a",
            "session-a[n]"
        ).orElseThrow();

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
        Attribute userColumn = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchReductionPlanner.HANDLE_ATTRIBUTE_NAME,
            DataType.KEYWORD
        );
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(userColumn),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(userColumn), false)
        );

        assertFalse(RemoteFetchReductionPlanner.needsRetainedSearchContexts(plan));
        assertTrue(RemoteFetchReductionPlanner.planReduceDriverTopN(contextFactory(), plan, "node-a", "session-a[n]").isEmpty());
    }

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
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
