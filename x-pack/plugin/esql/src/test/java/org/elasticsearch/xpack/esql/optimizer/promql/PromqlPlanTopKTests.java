/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanTopKTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanTopKTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeTopkEnabled() {
        assumeTrue("Requires PROMQL_TOPK capability", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        assumeTrue("Requires PROMQL_BOTTOMK capability", EsqlCapabilities.Cap.PROMQL_BOTTOMK.isEnabled());
    }

    public void testTopkProducesDescendingTopNBy() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.order(), hasSize(1));
        assertThat(topNBy.order().get(0).direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(((Number) topNBy.limitPerGroup().fold(FoldContext.small())).intValue(), equalTo(2));
    }

    public void testBottomkProducesAscendingTopNBy() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(bottomk(2, network.bytes_in))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.order(), hasSize(1));
        assertThat(topNBy.order().get(0).direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(((Number) topNBy.limitPerGroup().fold(FoldContext.small())).intValue(), equalTo(2));
    }

    /** Prometheus converts k with an integer cast: {@code topk(1.5, v)} keeps one series and {@code topk(0.5, v)} none. */
    public void testFractionalKIsTruncated() {
        for (var kAndLimit : List.of(Map.entry("1.5", 1), Map.entry("2.9", 2), Map.entry("0.5", 0), Map.entry("2", 2))) {
            for (String function : List.of("topk", "bottomk", "limitk")) {
                String promql = function + "(" + kAndLimit.getKey() + ", network.bytes_in)";
                var plan = logicalOptimizerWithLatestVersion.optimize(
                    planPromql("PROMQL index=k8s step=1h result=(" + promql + ")", false)
                );
                var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
                assertThat(promql, ((Number) topNBy.limitPerGroup().fold(FoldContext.small())).intValue(), equalTo(kAndLimit.getValue()));
            }
        }
    }

    /**
     * Unlike {@code sum(...)}, whose {@code NONE} grouping collapses every series into a single scalar, {@code topk}'s
     * ranking only trims the series count - the winning series keep their full label identity.
     */
    public void testTopkBareKeepsFullSeriesIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    public void testBottomkBareKeepsFullSeriesIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(bottomk(2, network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    /**
     * {@code topk(...) by (pod)} partitions the ranking by {@code pod} but, unlike {@code sum(...) by (pod)}, does
     * not drop every other label from the output - the winning series keep their full identity. The partition
     * itself needs {@code pod} resolved as a concrete column alongside the {@code _timeseries} full-identity key.
     */
    public void testTopkByGroupingPartitionsByLabelAndKeepsFullIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in) by (pod))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(partition(topNBy), equalTo(List.of("pod")));
    }

    public void testTopkByGroupingOnlyMaterializesPartitionLabel() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in) by (pod))", false);

        var dimensions = plan.collect(TimeSeriesAggregate.class)
            .stream()
            .flatMap(aggregate -> packedDims(aggregate.aggregates()).stream())
            .map(e -> e instanceof Attribute attribute ? attribute.name() : e.toString())
            .toList();
        assertThat(dimensions, equalTo(List.of(MetadataAttribute.TIMESERIES, "pod")));
    }

    public void testBottomkByGroupingPartitionsByLabelAndKeepsFullIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(bottomk(2, network.bytes_in) by (pod))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(partition(topNBy), equalTo(List.of("pod")));
    }

    public void testTopkWithoutGroupingPartitionsByTheRest() {
        assertWithoutPartitionsByTheRest("topk");
    }

    public void testBottomkWithoutGroupingPartitionsByTheRest() {
        assertWithoutPartitionsByTheRest("bottomk");
    }

    /**
     * {@code without (pod)} partitions the ranking by every label but {@code pod} and the metric name, as Prometheus groups a
     * {@code without}: the series' label container leaving them out. The winning series keep their full identity. A keyword label container
     * cannot derive that view beside the full one, so it requires a typed label container.
     */
    private void assertWithoutPartitionsByTheRest(String function) {
        String query = "PROMQL index=k8s step=1h result=(" + function + "(2, network.bytes_in) without (pod))";
        if (typedLabelContainer() == false) {
            var e = expectThrows(VerificationException.class, () -> planPromql(query, true));
            assertThat(e.getMessage(), containsString("requires a typed label container"));
            return;
        }
        var plan = logicalOptimizerWithLatestVersion.optimize(planPromql(query, false));
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.groupings(), hasSize(2));
        assertThat(topNBy.groupings().get(1).dataType(), equalTo(DataType.TSMD_DATA_TYPE));
    }

    /** Over named labels {@code without} partitions by the labels it does not list, whatever the label container representation. */
    public void testTopkWithoutGroupingOverNamedLabelsPartitionsByTheRest() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(1, sum by (pod, region) (network.bytes_in)) without (pod))", false)
        );
        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(partition(topNBy), equalTo(List.of("region")));
    }

    /**
     * {@code topk} over an already-aggregated vector (e.g. {@code sum by}) ranks the aggregate's rows as they are: it adds no
     * aggregate of its own, so the aggregate below it is the {@code sum}.
     */
    public void testTopkOverSumByRanksAggregateRows() {
        assumeTrue("Requires FIX_PROMQL_TOPK_OVER_AGGREGATE capability", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(2, sum by (pod) (network.bytes_in)))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        Aggregate outer = as(topNBy.child().collect(Aggregate.class).get(0), Aggregate.class);
        NamedExpression valueAgg = outer.aggregates().get(0);
        assertThat(Alias.unwrap(valueAgg), instanceOf(Sum.class));
    }

    public void testBottomkOverSumByRanksAggregateRows() {
        assumeTrue("Requires FIX_PROMQL_TOPK_OVER_AGGREGATE capability", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(bottomk(2, sum by (pod) (network.bytes_in)))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        Aggregate outer = as(topNBy.child().collect(Aggregate.class).get(0), Aggregate.class);
        NamedExpression valueAgg = outer.aggregates().get(0);
        assertThat(Alias.unwrap(valueAgg), instanceOf(Sum.class));
    }

    public void testLimitkProducesEmptyOrderTopNBy() {
        assumeTrue("Requires PROMQL_LIMITK capability", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limitk(2, network.bytes_in))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.order(), hasSize(0));
        assertThat(((Number) topNBy.limitPerGroup().fold(FoldContext.small())).intValue(), equalTo(2));
    }

    /**
     * Unlike {@code topk}/{@code bottomk} which rank by value, {@code limitk} keeps the first-k series in storage
     * order - the winning series still keep their full label identity.
     */
    public void testLimitkBareKeepsFullSeriesIdentity() {
        assumeTrue("Requires PROMQL_LIMITK capability", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limitk(2, network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    public void testLimitkByGroupingPartitionsByLabelAndKeepsFullIdentity() {
        assumeTrue("Requires PROMQL_LIMITK capability", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limitk(2, network.bytes_in) by (pod))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.order(), hasSize(0));
        assertThat(partition(topNBy), equalTo(List.of("pod")));
    }

    public void testLimitkWithoutGroupingPartitionsByTheRest() {
        assumeTrue("Requires PROMQL_LIMITK capability", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());
        assertWithoutPartitionsByTheRest("limitk");
    }

    public void testLimitkOverSumByRanksAggregateRows() {
        assumeTrue("Requires PROMQL_LIMITK capability", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());
        assumeTrue("Requires FIX_PROMQL_TOPK_OVER_AGGREGATE capability", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limitk(2, sum by (pod) (network.bytes_in)))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        assertThat(topNBy.order(), hasSize(0));
        Aggregate outer = as(topNBy.child().collect(Aggregate.class).get(0), Aggregate.class);
        NamedExpression valueAgg = outer.aggregates().get(0);
        assertThat(Alias.unwrap(valueAgg), instanceOf(Sum.class));
    }

    /** The labels {@code topNBy} partitions by, besides the step: they pack into one key. */
    private static List<String> partition(TopNBy topNBy) {
        PackDims pack = as(topNBy.child().collect(PackDims.class).getFirst(), PackDims.class);
        assertThat(topNBy.groupings(), hasItem(pack.packed()));
        return pack.dims().stream().map(Attribute::name).toList();
    }
}
