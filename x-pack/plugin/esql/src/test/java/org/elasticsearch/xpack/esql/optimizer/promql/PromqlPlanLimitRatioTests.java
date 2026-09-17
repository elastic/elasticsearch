/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LimitRatioBy;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanLimitRatioTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanLimitRatioTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeLimitRatioEnabled() {
        assumeTrue("Requires PROMQL_LIMIT_RATIO capability", EsqlCapabilities.Cap.PROMQL_LIMIT_RATIO.isEnabled());
    }

    /**
     * {@code limit_ratio} over an aggregate samples result series, not raw series: the input rows are
     * groups carrying no {@code _timeseries}, so the series key must be the group packing -- a real
     * column -- and never a constant (a constant key would keep or drop every group together).
     */
    public void testLimitRatioOverAggregateUsesGroupCarriersAsFieldKey() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod) (network.total_bytes_in{cluster=\"prod\"})))", false)
        );

        var node = as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
        assertThat(node.fieldKey().foldable(), equalTo(false));
        assertThat(node.child().output(), hasItem((Attribute) node.fieldKey()));
        var eval = as(node.child(), org.elasticsearch.xpack.esql.plan.logical.Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).child().foldable(), equalTo(false));
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

    /**
     * Like the other reductions ({@code TopNBy}) the node is a {@link PipelineBreaker} running on the
     * coordinator after collection, but the hash predicate itself is per-series stateless: it needs no
     * global per-group view, so unlike before it must not claim {@link ExecutesOn.Coordinator}.
     */
    public void testLimitRatioPlacedLikeTopK() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))", false)
        );

        var node = as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
        assertThat(node, instanceOf(PipelineBreaker.class));
        assertThat(node instanceof ExecutesOn.Coordinator, equalTo(false));
    }

    /**
     * Like Prometheus, a negative ratio is accepted and keeps the complement subset
     * (offsets at or above {@code 1 + r}).
     */
    public void testLimitRatioNegativeAcceptedAsComplement() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(-0.5, network.bytes_in))", false)
        );

        var node = as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
        assertThat(((Number) node.ratio().fold(FoldContext.small())).doubleValue(), closeTo(-0.5, 1e-10));
    }

    /**
     * Like Prometheus, NaN ratios are rejected; infinite ratios clamp naturally (+Inf keeps everything).
     */
    public void testLimitRatioNaNRejected() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(nan, network.bytes_in))", true)
        );
        assertThat(e.getMessage(), containsString("must not be NaN"));
    }

    /**
     * Like Prometheus, infinite ratios are accepted and clamp naturally (+Inf keeps everything).
     */
    public void testLimitRatioInfiniteAccepted() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(Inf, network.bytes_in))", false)
        );

        assertThat(plan.collect(LimitRatioBy.class).get(0), instanceOf(LimitRatioBy.class));
    }

    public void testLimitRatioStringRejected() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(\"0.5\", network.bytes_in))", true)
        );
        assertThat(e.getMessage(), containsString("numeric ratio"));
    }

    /**
     * Translator output passes post-optimization verification: the ratio is a numeric literal and
     * the field key is a resolved keyword attribute of the input.
     */
    public void testLimitRatioVerificationAcceptsTranslatorOutput() {
        var failures = new Failures();
        limitRatioByNode().postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(false));
    }

    /**
     * Post-optimization verification rejects a non-numeric ratio with source context instead of
     * failing deep in execution planning.
     */
    public void testLimitRatioVerificationRejectsNonNumericRatio() {
        var node = limitRatioByNode();
        var bad = new LimitRatioBy(
            node.source(),
            node.child(),
            new Literal(node.source(), new BytesRef("0.5"), DataType.KEYWORD),
            node.groupings(),
            node.fieldKey()
        );
        var failures = new Failures();
        bad.postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(true));
        assertThat(failures.toString(), containsString("must be a numeric literal"));
    }

    public void testLimitRatioVerificationRejectsNaNRatio() {
        var node = limitRatioByNode();
        var bad = new LimitRatioBy(
            node.source(),
            node.child(),
            new Literal(node.source(), Double.NaN, DataType.DOUBLE),
            node.groupings(),
            node.fieldKey()
        );
        var failures = new Failures();
        bad.postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(true));
        assertThat(failures.toString(), containsString("must not be NaN"));
    }

    public void testLimitRatioVerificationRejectsNonAttributeFieldKey() {
        var node = limitRatioByNode();
        var bad = new LimitRatioBy(
            node.source(),
            node.child(),
            node.ratio(),
            node.groupings(),
            new Literal(node.source(), new BytesRef("key"), DataType.KEYWORD)
        );
        var failures = new Failures();
        bad.postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(true));
        assertThat(failures.toString(), containsString("must be an attribute"));
    }

    public void testLimitRatioVerificationRejectsUnresolvableFieldKey() {
        var node = limitRatioByNode();
        var bad = new LimitRatioBy(
            node.source(),
            node.child(),
            node.ratio(),
            node.groupings(),
            new ReferenceAttribute(node.source(), "missing", DataType.KEYWORD)
        );
        var failures = new Failures();
        bad.postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(true));
        assertThat(failures.toString(), containsString("is not produced by its input"));
    }

    public void testLimitRatioVerificationRejectsNonKeywordFieldKey() {
        var node = limitRatioByNode();
        Attribute step = node.child()
            .output()
            .stream()
            .filter(a -> a.name().equals("step"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a step column"));
        var bad = new LimitRatioBy(node.source(), node.child(), node.ratio(), node.groupings(), step);
        var failures = new Failures();
        bad.postOptimizationVerification(failures);
        assertThat(failures.hasFailures(), equalTo(true));
        assertThat(failures.toString(), containsString("must be a keyword"));
    }

    private LimitRatioBy limitRatioByNode() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))", false)
        );
        return as(plan.collect(LimitRatioBy.class).get(0), LimitRatioBy.class);
    }
}
