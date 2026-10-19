/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/** Reductions partition complete typed vectors without changing the identity of retained series. */
public class FunctionalReductionTranslationTests extends ESTestCase {
    public void testReductionsComposeWithRelabelsJoinsAndSetOperations() {
        for (String reduction : List.of("topk(2,", "bottomk(2,", "limitk(2,", "limit_ratio(0.5,")) {
            for (String operand : List.of(
                "label_replace(network.bytes_in, \"copy\", \"$1\", \"pod\", \"(.*)\")",
                "network.bytes_in / on(cluster) group_left sum by(cluster)(network.eth0.rx)",
                "network.bytes_in and on(cluster) network.eth0.rx"
            )) {
                LogicalPlan plan = analyze(reduction + operand + ")");
                assertEquals(List.of("result", "step", "_timeseries"), plan.output().stream().map(Attribute::name).toList());
            }
        }
        analyze("label_join(topk(2, network.bytes_in), \"copy\", \"/\", \"cluster\", \"pod\")");
        analyze("topk(2, network.bytes_in) / on(cluster) group_left sum by(cluster)(network.eth0.rx)");
    }

    public void testByPartitionsUseOnlyRequestedLabelsAndStep() {
        LogicalPlan plan = analyze("topk by(cluster)(2, network.bytes_in)");
        TopNBy reduction = onlyReduction(plan);
        assertEquals(2, reduction.groupings().size());
        assertEquals(DataType.DATETIME, reduction.groupings().getFirst().dataType());
        TimeSeriesHash partition = partitionKey(reduction);
        assertNull(as(partition.metadata(), Literal.class).value());
        assertEquals(List.of("cluster"), names(partition));
        assertEquals(List.of("result", "step", "_timeseries"), plan.output().stream().map(Attribute::name).toList());
    }

    public void testWithoutPartitionsExcludeMetricNameButRetainOutputIdentity() {
        LogicalPlan plan = analyze("topk without(pod)(2, network.bytes_in)");
        TimeSeriesHash partition = partitionKey(onlyReduction(plan));
        TimeSeriesUnset metadata = as(partition.metadata(), TimeSeriesUnset.class);
        assertEquals(
            List.of(LabelMatcher.NAME, "pod"),
            metadata.children()
                .subList(1, metadata.children().size())
                .stream()
                .map(e -> as(e.fold(FoldContext.small()), BytesRef.class).utf8ToString())
                .toList()
        );
        assertEquals(List.of("result", "step", "_timeseries"), plan.output().stream().map(Attribute::name).toList());

        // Finite inputs have no residual dictionary: surviving promoted labels still define the partition.
        plan = analyze("bottomk without(pod)(1, sum by(cluster,pod)(network.bytes_in))");
        partition = partitionKey(onlyReduction(plan));
        assertNull(as(partition.metadata(), Literal.class).value());
        assertEquals(List.of("cluster"), names(partition));
        assertEquals(List.of("result", "step", "cluster", "pod"), plan.output().stream().map(Attribute::name).toList());
    }

    public void testMissingPartitionLabelAndEmptyPartitionRemainValid() {
        for (String expression : List.of(
            "topk by(cluster)(1, vector(7))",
            "bottomk without(cluster)(1, vector(7))",
            "limitk by()(1, network.bytes_in)",
            "limitk without()(1, network.bytes_in)",
            "topk by(copy)(1, label_replace(network.bytes_in, \"copy\", \"\", \"pod\", \".*\"))"
        )) {
            TopNBy reduction = onlyReduction(analyze(expression));
            assertEquals(2, reduction.groupings().size());
            assertEquals(DataType.TSID_DATA_TYPE, reduction.groupings().getLast().dataType());
        }
    }

    public void testLimitRatioSamplesCompleteCanonicalIdentity() {
        TimeSeriesHash raw = samplingKey(analyze("limit_ratio(0.5, network.bytes_in)"));
        assertEquals(DataType.TIMESERIES_DATA_TYPE, raw.metadata().dataType());
        assertTrue(names(raw).isEmpty());

        // An enclosing BY promotes a label, but the sampling key must put it back beside the residual metadata.
        TimeSeriesHash promoted = samplingKey(analyze("sum by(cluster)(limit_ratio(0.5, network.bytes_in))"));
        assertEquals(DataType.TIMESERIES_DATA_TYPE, promoted.metadata().dataType());
        assertEquals(List.of("cluster"), names(promoted));

        TimeSeriesHash finite = samplingKey(analyze("limit_ratio(0.5, sum by(pod,cluster)(network.bytes_in))"));
        assertNull(as(finite.metadata(), Literal.class).value());
        assertEquals(List.of("cluster", "pod"), names(finite));
    }

    public void testLimitRatioGroupingDoesNotChangeSamplingKeys() {
        for (String grouping : List.of("", " by(cluster)", " without(pod)")) {
            LogicalPlan plan = analyze("limit_ratio" + grouping + "(0.5, network.bytes_in)");
            TimeSeriesHash key = samplingKey(plan);
            assertEquals(DataType.TIMESERIES_DATA_TYPE, key.metadata().dataType());
            assertTrue(names(key).isEmpty());
            assertTrue(plan.collect(TopNBy.class).isEmpty());
        }
    }

    public void testZeroLimitsDiscardTheirInput() {
        for (String reduction : List.of("topk(0,", "bottomk(-1,", "limitk(0.5,", "limit_ratio(0,")) {
            LogicalPlan plan = analyze(reduction + "label_replace(network.bytes_in, \"pod\", \"same\", \"pod\", \".*\")) or vector(2)");
            assertTrue(plan.collect(Filter.class).stream().anyMatch(filter -> Literal.FALSE.equals(filter.condition())));
            assertTrue(plan.collect(TopNBy.class).isEmpty());
        }
    }

    public void testConstantVectorsAreReducedOnEveryStep() {
        for (String reduction : List.of("topk(1,", "bottomk(1,", "limitk(1,")) {
            LogicalPlan plan = analyze(reduction + "vector(7))");
            assertEquals(1, plan.collect(TopNBy.class).size());
            assertTrue(plan.collect(EsRelation.class).isEmpty());
        }
        LogicalPlan empty = analyze("topk(0, vector(7)) or vector(2)");
        assertTrue(empty.collect(Filter.class).stream().anyMatch(filter -> Literal.FALSE.equals(filter.condition())));
        assertTrue(empty.collect(EsRelation.class).isEmpty());
    }

    public void testHistogramSamplesAreKeptOnlyBySamplingReductions() {
        TestAnalyzer histograms = prototype().addIndex("histograms", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES);
        for (String reduction : List.of("topk(1,", "bottomk(1,")) {
            LogicalPlan plan = histograms.query(query(reduction + "responseTime)").replace("index=k8s", "index=histograms"));
            assertConsistent(plan);
            assertTrue(plan.collect(Filter.class).stream().anyMatch(filter -> Literal.FALSE.equals(filter.condition())));
            assertEquals(DataType.DOUBLE, plan.output().getFirst().dataType());
        }
        for (String reduction : List.of("limitk(1,", "limit_ratio(0.5,")) {
            LogicalPlan plan = histograms.query(
                query("histogram_count(" + reduction + "responseTime))").replace("index=k8s", "index=histograms")
            );
            assertConsistent(plan);
            var components = new ArrayList<ExtractHistogramComponent>();
            plan.forEachExpressionDown(ExtractHistogramComponent.class, components::add);
            assertFalse(components.isEmpty());
            assertTrue(components.stream().allMatch(component -> component.field().dataType() == DataType.EXPONENTIAL_HISTOGRAM));
        }
    }

    public void testOlderClustersRetainLegacyReductionsAndRestrictions() {
        for (TestAnalyzer legacy : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
        )) {
            LogicalPlan plan = legacy.query(query("topk by(cluster)(2, network.bytes_in)"));
            assertConsistent(plan);
            plan.forEachExpressionDown(TimeSeriesFunction.class, function -> fail("Legacy reduction must not use typed metadata"));
            VerificationException failure = expectThrows(
                VerificationException.class,
                () -> legacy.query(query("topk without(pod)(2, network.bytes_in)"))
            );
            assertThat(failure.getMessage(), containsString("is not yet supported with [WITHOUT]"));
            legacy.error(
                query("topk(2, label_replace(network.bytes_in, \"copy\", \"$1\", \"pod\", \"(.*)\"))"),
                containsString("only supported inside a `by(...)` aggregation")
            );
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().addK8s().minimumTransportVersion(featureVersion()).flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }

    private static LogicalPlan analyze(String expression) {
        try {
            LogicalPlan plan = prototype().query(query(expression));
            assertConsistent(plan);
            return plan;
        } catch (RuntimeException e) {
            throw new AssertionError(expression, e);
        }
    }

    private static TopNBy onlyReduction(LogicalPlan plan) {
        assertEquals(1, plan.collect(TopNBy.class).size());
        return plan.collect(TopNBy.class).getFirst();
    }

    private static TimeSeriesHash partitionKey(TopNBy reduction) {
        Expression key = reduction.groupings().getLast();
        var definitions = new HashMap<NameId, Expression>();
        reduction.child().forEachExpressionDown(Alias.class, alias -> definitions.put(alias.id(), alias.child()));
        while (key instanceof Attribute attribute && definitions.containsKey(attribute.id())) {
            key = definitions.remove(attribute.id());
        }
        return as(key, TimeSeriesHash.class);
    }

    private static TimeSeriesHash samplingKey(LogicalPlan plan) {
        var offsets = new ArrayList<HashOffset>();
        plan.forEachExpressionDown(HashOffset.class, offsets::add);
        assertEquals(1, offsets.size());
        assertEquals(1, offsets.getFirst().children().size());
        return as(offsets.getFirst().children().getFirst(), TimeSeriesHash.class);
    }

    private static List<String> names(TimeSeriesHash key) {
        var names = new ArrayList<String>();
        for (int i = 1; i < key.children().size(); i += 2) {
            names.add(as(key.children().get(i).fold(FoldContext.small()), BytesRef.class).utf8ToString());
        }
        return names;
    }

    private static void assertConsistent(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
        plan.forEachUp(node -> assertTrue(node.output().stream().filter(a -> a.dataType() == DataType.TIMESERIES_DATA_TYPE).count() <= 1));
    }

    private static String query(String expression) {
        return "PROMQL index=k8s start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=("
            + expression
            + ") | LIMIT 100";
    }
}
