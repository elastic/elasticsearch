/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.LimitRatioBy;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanLimitRatioTests extends AbstractPromqlPlanOptimizerTests {

    @Before
    public void assumeLimitRatioEnabled() {
        assumeTrue("Requires PROMQL_LIMIT_RATIO capability", EsqlCapabilities.Cap.PROMQL_LIMIT_RATIO.isEnabled());
    }

    public void testLimitRatioProducesLimitRatioBy() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))", false)
        );

        var node = as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
        assertThat(((Number) node.ratio().fold(FoldContext.small())).doubleValue(), closeTo(0.5, 1e-10));
    }

    /**
     * Unlike aggregations, {@code limit_ratio} keeps the full label identity of each selected series.
     */
    public void testLimitRatioBareKeepsFullSeriesIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.3, network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    /**
     * {@code limit_ratio(...) by (pod)} must resolve {@code pod} as a concrete column
     * alongside the {@code _timeseries} full-identity key and keep full series identity.
     */
    public void testLimitRatioByGroupingPartitionsByLabelAndKeepsFullIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (pod))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));

        var node = as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
        assertThat(node.groupings().stream().map(g -> g instanceof Attribute a ? a.name() : g.toString()).toList(), hasItem("pod"));
    }

    public void testLimitRatioWithoutGroupingNotYetSupported() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) without (pod))", true)
        );
        assertThat(e.getMessage(), containsString("limit_ratio"));
    }

    public void testLimitRatioNodeType() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.1, network.bytes_in))", false)
        );

        assertThat(plan.collect(LimitRatioBy.class).get(0), instanceOf(LimitRatioBy.class));
    }
}
