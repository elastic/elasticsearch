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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.FloatToLabelValue;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Plan shape of PromQL {@code count_values}: one series-grain collapse of a raw child, a regroup counting each distinct
 * double value, and the label text formatted once per output group.
 */
public class PromqlPlanCountValuesTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanCountValuesTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeCountValuesEnabled() {
        assumeTrue("Requires PROMQL_COUNT_VALUES capability", EsqlCapabilities.Cap.PROMQL_COUNT_VALUES.isEnabled());
    }

    /** Every NaN payload groups as one value: packed multi-key hashing compares the raw bits of a double. */
    public void testNaNKeyIsCanonicalized() {
        LogicalPlan plan = countValuesPlan("count_values(\"v\", network.cost)");
        Case nanCase = as(groupingKeyAlias(plan).child(), Case.class);
        NotEquals isNaN = as(nanCase.children().get(0), NotEquals.class);
        assertTrue(isNaN.left().semanticEquals(isNaN.right()));
        Literal nan = as(nanCase.children().get(1), Literal.class);
        assertTrue(Double.isNaN((Double) nan.value()));
        assertTrue(nanCase.children().get(2).semanticEquals(isNaN.left()));
    }

    /** A whole number never converts to NaN, so its key is the plain double conversion. */
    public void testWholeNumberKeyIsNotCanonicalized() {
        for (String metric : List.of("network.bytes_in", "network.total_bytes_in", "network.eth0.tx")) {
            LogicalPlan plan = countValuesPlan("count_values(\"v\", " + metric + ")");
            assertThat(metric, groupingKeyAlias(plan).child(), instanceOf(ToDouble.class));
        }
    }

    /** A group whose series have no value at a step counts 0, which is no element: the count maps to null above it. */
    public void testZeroCountIsNull() {
        LogicalPlan plan = countValuesPlan("count_values(\"v\", network.bytes_in)");
        Aggregate countAggregate = countAggregate(plan);
        Eval above = plan.collect(Eval.class).stream().filter(e -> e.child() == countAggregate).findFirst().orElseThrow();
        Case zeroAsNull = as(above.fields().getFirst().child(), Case.class);
        Equals isZero = as(zeroAsNull.children().getFirst(), Equals.class);
        assertEquals(0L, as(isZero.right(), Literal.class).value());
    }

    public void testAggregatedChildIsNotCollapsedAgain() {
        LogicalPlan plan = countValuesPlan("count_values(\"v\", sum by (cluster) (network.bytes_in))");
        assertThat(plan.collect(TimeSeriesAggregate.class), hasSize(1));
        assertEquals(List.of("step", "v"), groupingNames(countAggregate(plan)));
    }

    /** A key that is not a dimension groups the per-series values too, as for {@code sum by}. */
    public void testNonDimensionKey() {
        LogicalPlan plan = countValuesPlan("count_values by (event) (\"v\", network.bytes_in)");
        assertEquals(List.of("step", "event", "v"), groupingNames(countAggregate(plan)));
        List<Aggregate> regroups = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(regroups, hasSize(2));
        for (Aggregate regroup : regroups) {
            assertThat(groupingNames(regroup), hasItem("event"));
        }
    }

    /** A value label written with the passthrough prefix names the same label as its bare form. */
    public void testPrefixedValueLabel() {
        LogicalPlan plan = planPrometheusLayout("count_values(\"labels.instance\", sum by (instance, job) (node_memory_MemTotal_bytes))");
        Attribute key = groupingAttribute(countAggregate(plan), "instance");
        assertThat(key, not(instanceOf(FieldAttribute.class)));
        assertEquals(DataType.DOUBLE, key.dataType());
        Alias label = formattedLabel(plan, "labels.instance");
        assertEquals(key.id(), as(label.child().children().getFirst(), Attribute.class).id());
        assertEquals(List.of("labels.instance"), plan.output().stream().skip(2).map(Attribute::name).toList());
    }

    /**
     * A constant child folds without any aggregate: the label is formatted from the constant value, and a key the child
     * already carries is kept instead of being null-filled.
     */
    public void testFoldedConstant() {
        LogicalPlan plan = planPromql(
            "PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" count_values by (v) (\"w\", count_values(\"v\", vector(1)))",
            false,
            false
        );
        assertThat(plan.collect(Aggregate.class), empty());
        assertEquals(List.of("step", "v", "w"), outputNames(plan));
        assertEquals(new BytesRef("1"), formattedLabel(plan, "v").child().fold(FoldContext.small()));
        for (String name : List.of("v", "w")) {
            assertEquals(formattedLabel(plan, name).id(), output(plan, name).id());
        }
    }

    public void testCompositionsTranslate() {
        for (String promql : List.of(
            "topk(1, count_values(\"v\", network.bytes_in))",
            "count_values(\"v\", network.bytes_in) or network.cost",
            "count_values(\"v\", sum without (pod) (network.bytes_in))",
            "count_values by (w) (\"v\", label_replace(network.bytes_in, \"w\", \"$1\", \"pod\", \"(.*)\"))"
        )) {
            LogicalPlan plan = countValuesPlan(promql);
            assertThat(promql, outputNames(plan), hasItem("v"));
        }
    }

    public void testRelabelOverCountValuesAddsLabel() {
        LogicalPlan plan = countValuesPlan("label_replace(count_values(\"v\", network.bytes_in), \"w\", \"$1\", \"v\", \"(.*)\")");
        assertEquals(List.of("step", "v", "w"), outputNames(plan));
    }

    public void testNumberOperandStaysFused() {
        LogicalPlan plan = countValuesPlan("count_values(\"v\", network.bytes_in) * 2");
        assertThat(plan.collect(InnerJoin.class), empty());
        assertEquals(List.of("step", "v"), outputNames(plan));
    }

    /**
     * Operands with equal label sets fold into one shared aggregate by default; a {@code count_values} operand is two
     * aggregation levels deep, so it is translated on its own and joined, also next to a single-level aggregate.
     */
    public void testCountValuesOperandIsJoined() {
        LogicalPlan plan = countValuesPlan("count_values(\"cluster\", network.bytes_in) / sum by (cluster) (network.cost)");
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    /** The fused path cannot pair an aggregated constant with {@code scalar(...)} of a vector. */
    public void testConstantTimesScalarOfVectorIsRejected() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> countValuesPlan("count_values(\"v\", vector(1)) * scalar(sum(network.bytes_in))")
        );
        assertThat(e.getMessage(), containsString("binary operations between vectors with mismatched grouping keys are not yet supported"));
    }

    private LogicalPlan countValuesPlan(String promql) {
        return planPromql("PROMQL index=k8s step=1h " + promql, false);
    }

    /** Output names after the value column, which is named after the query text. */
    private static List<String> outputNames(LogicalPlan plan) {
        return plan.output().stream().skip(1).map(Attribute::name).toList();
    }

    private static Attribute output(LogicalPlan plan, String name) {
        return plan.output().stream().filter(a -> a.name().equals(name)).findFirst().orElseThrow();
    }

    /** The aggregate counting the distinct values: the plain aggregate whose value is a {@link Count}. */
    private static Aggregate countAggregate(LogicalPlan plan) {
        List<Aggregate> counts = plan.collect(Aggregate.class)
            .stream()
            .filter(a -> a instanceof TimeSeriesAggregate == false)
            .filter(a -> a.aggregates().stream().anyMatch(e -> e instanceof Alias alias && alias.child() instanceof Count))
            .toList();
        assertThat(counts, hasSize(1));
        return counts.getFirst();
    }

    private static List<String> groupingNames(Aggregate aggregate) {
        return aggregate.groupings().stream().map(g -> ((NamedExpression) g).name()).toList();
    }

    private static Attribute groupingAttribute(Aggregate aggregate, String name) {
        return aggregate.groupings()
            .stream()
            .map(g -> ((NamedExpression) g).toAttribute())
            .filter(a -> a.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no grouping [" + name + "] in " + aggregate.groupings()));
    }

    /** The alias defining the double key that the counting aggregate groups by. */
    private static Alias groupingKeyAlias(LogicalPlan plan) {
        Attribute key = groupingAttribute(countAggregate(plan), "v");
        return plan.collect(Eval.class)
            .stream()
            .flatMap(e -> e.fields().stream())
            .filter(a -> a.id().equals(key.id()))
            .findFirst()
            .orElseThrow();
    }

    /** The alias formatting the label {@code name} from its double value. */
    private static Alias formattedLabel(LogicalPlan plan, String name) {
        List<Alias> formatted = plan.collect(Eval.class)
            .stream()
            .flatMap(e -> e.fields().stream())
            .filter(a -> a.name().equals(name) && a.child() instanceof FloatToLabelValue)
            .toList();
        assertThat(formatted, hasSize(1));
        return formatted.getFirst();
    }
}
