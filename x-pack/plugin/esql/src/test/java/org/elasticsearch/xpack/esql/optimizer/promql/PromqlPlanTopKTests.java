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
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanTopKTests extends AbstractPromqlPlanOptimizerTests {

    @Before
    public void assumeTopkEnabled() {
        assumeTrue("Requires PROMQL_TOPK capability", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
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
        assertThat(topNBy.groupings().stream().map(g -> g instanceof Attribute a ? a.name() : g.toString()).toList(), hasItem("pod"));
    }

    public void testTopkWithoutGroupingNotYetSupported() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in) without (pod))", true)
        );
        assertThat(e.getMessage(), containsString("topk"));
    }

    /**
     * {@code topk} over an already-aggregated vector (e.g. {@code sum by}) must wrap the passthrough value in
     * {@link Values} in the outer aggregate. Without that, physical planning drops the bare attribute from the
     * layout and fails with {@code can't find input for [topk(...)]}.
     */
    public void testTopkOverSumByWrapsPassthroughInValues() {
        assumeTrue("Requires FIX_PROMQL_TOPK_OVER_AGGREGATE capability", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());

        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(topk(2, sum by (pod) (network.bytes_in)))", false)
        );

        var topNBy = as(plan.collect(TopNBy.class).get(0), TopNBy.class);
        Aggregate outer = as(topNBy.child().collect(Aggregate.class).get(0), Aggregate.class);
        NamedExpression valueAgg = outer.aggregates().get(0);
        assertThat(Alias.unwrap(valueAgg), instanceOf(Values.class));
    }
}
