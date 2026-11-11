/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PushDownPromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * How {@link PushDownPromqlLabels} splits the label container of a translated plan into columns: which keys a collapse and an
 * aggregate group by, how joins compare labels, and that no label is read off a container once the plan names it.
 */
public class PromqlPlanLabelPushdownTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanLabelPushdownTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumePromql() {
        assumeTrue("Requires PROMQL_COMMAND_V0 capability", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
    }

    /** Labels the plan names are columns: nothing reads, keeps or writes them in a label container. */
    public void testNamedLabelsAreColumns() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h result=(sum by (pod) (rate(network.total_bytes_in[5m])))", false);
        assertThat(containerCalls(plan), empty());
    }

    /**
     * A collapse keeping one row per series that nothing reads a view of keys by the series id: the label it rewrites is the only
     * label read above it, so the source reads no view of its series.
     */
    public void testPerSeriesCollapseKeysBySeriesIdWhenNoViewIsRead() {
        assumeTrue("Requires PROMQL_LABEL_FUNCTIONS capability", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (tier) (label_replace(network.bytes_in, \"tier\", \"$1\", \"region\", \"(.+)\")))",
            false
        );
        assertThat(views(plan), empty());
        assertTrue(
            plan.collect(Aggregate.class)
                .stream()
                .filter(aggregate -> aggregate instanceof TimeSeriesAggregate == false)
                .anyMatch(aggregate -> names(aggregate.groupings()).contains(MetadataAttribute.TSID_FIELD))
        );
    }

    /** A collapse whose series are rendered as a whole carries one view of the source. */
    public void testPerSeriesCollapseCarriesOneView() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h result=(network.bytes_in or network.cost)", false);
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertThat(relation.output().stream().filter(TimeSeriesMetadataAttribute.class::isInstance).toList(), hasSize(1));
        }
    }

    /** An aggregate over labels packs them into one key, so a label of several values keys as a whole. */
    public void testAggregateKeysByPackedLabels() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1h result=(sum by (cluster) (sum without (pod) (sum by (cluster, region, pod) (network.cost))))",
            false
        );
        List<Aggregate> regroups = plan.collect(Aggregate.class)
            .stream()
            .filter(aggregate -> aggregate instanceof TimeSeriesAggregate == false)
            .toList();
        assertThat(regroups, not(empty()));
        for (Aggregate aggregate : regroups) {
            if (aggregate.child() instanceof TimeSeriesAggregate == false) {
                assertThat(aggregate.child(), instanceOf(PackDims.class));
            }
        }
        List<List<String>> packed = plan.collect(PackDims.class).stream().map(pack -> names(pack.dims())).toList();
        assertThat(packed, hasItem(List.of("cluster", "region")));
        assertThat(packed, hasItem(List.of("cluster")));
    }

    /** A join compares the labels it matches on packed, never a label container. */
    public void testJoinComparesPackedLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))",
            false
        );
        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(join.leftFields(), hasSize(2));
        assertThat(join.leftFields().getFirst().name(), equalTo("step"));
        assertThat(join.left().collect(PackDims.class).getFirst().packed(), equalTo(join.leftFields().get(1)));
        assertThat(join.leftFields().stream().filter(key -> key.dataType() == DataType.TIME_SERIES_METADATA_DATA_TYPE).toList(), empty());
    }

    private static List<Expression> containerCalls(LogicalPlan plan) {
        return plan.collect(node -> true)
            .stream()
            .flatMap(node -> node.expressions().stream())
            .flatMap(expression -> expression.collect(AbstractTimeSeriesMetadataFunction.class::isInstance).stream())
            .toList();
    }

    private static List<Attribute> views(LogicalPlan plan) {
        return plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .toList();
    }

    private static List<String> names(List<? extends Expression> expressions) {
        return expressions.stream()
            .map(expression -> expression instanceof NamedExpression named ? named.name() : expression.toString())
            .toList();
    }
}
