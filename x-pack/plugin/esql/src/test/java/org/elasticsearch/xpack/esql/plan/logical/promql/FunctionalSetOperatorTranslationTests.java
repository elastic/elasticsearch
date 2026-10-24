/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.containsString;

/** Set membership uses the same label hashes as vector matching without one-to-one cardinality constraints. */
public class FunctionalSetOperatorTranslationTests extends ESTestCase {
    public void testAndAndUnlessRetainEveryMatchingLeftSeries() {
        for (String operator : List.of("and", "unless")) {
            for (String matching : List.of("", "on(cluster)", "ignoring(pod)")) {
                LogicalPlan plan = analyze("network.bytes_in " + operator + " " + matching + " network.total_bytes_out");
                assertEquals(List.of("result", "step", "_timeseries"), plan.output().stream().map(Attribute::name).toList());
                assertEquals(1, plan.collect(AbstractSubqueryJoin.class).size());
                AbstractSubqueryJoin join = plan.collect(AbstractSubqueryJoin.class).getFirst();
                assertEquals(operator.equals("and") ? SemiJoin.class : AntiJoin.class, join.getClass());
                assertEquals(2, join.config().leftFields().size());
                assertEquals(DataType.DATETIME, join.config().leftFields().getFirst().dataType());
                assertEquals(DataType.TSID_DATA_TYPE, join.config().leftFields().getLast().dataType());
                assertEquals(join.config().rightFields(), ((Project) join.right()).projections());
                assertTrue(plan.collect(InnerJoin.class).isEmpty());
                assertIndependentSources(plan, 2);
                assertTrue(plan.collect(TopNBy.class).isEmpty());
            }
        }
    }

    public void testMembershipOutputUsesOnlyLeftLabels() {
        for (String operator : List.of("and", "unless")) {
            LogicalPlan plan = analyze("sum by(cluster)(network.bytes_in) " + operator + " on() sum by(pod)(network.total_bytes_out)");
            assertEquals(List.of("result", "step", "cluster"), plan.output().stream().map(Attribute::name).toList());
        }
    }

    public void testOrKeepsAllLeftRowsWithTheSameMatchingKey() {
        LogicalPlan plan = analyze("network.bytes_in or on(cluster) network.total_bytes_out");
        assertEquals(1, plan.collect(UnionAll.class).size());
        UnionAll union = plan.collect(UnionAll.class).getFirst();
        assertTrue(union.children().getFirst().collect(AbstractSubqueryJoin.class).isEmpty());
        assertTrue(union.children().getLast().collect(AbstractSubqueryJoin.class).isEmpty());
        assertEquals(1, plan.collect(InlineJoin.class).size());
        InlineJoin join = plan.collect(InlineJoin.class).getFirst();
        assertSame(union, join.left());
        assertEquals(2, join.config().leftFields().size());
        assertEquals(DataType.DATETIME, join.config().leftFields().getFirst().dataType());
        assertEquals(DataType.TSID_DATA_TYPE, join.config().leftFields().getLast().dataType());
        Aggregate aggregate = as(join.right(), Aggregate.class);
        as(aggregate.child(), StubRelation.class);
        assertEquals(join.config().leftFields(), aggregate.groupings());
        as(as(aggregate.aggregates().getFirst(), Alias.class).child(), Min.class);
        int keyColumn = union.output().indexOf(join.config().leftFields().getLast());
        assertTrue(keyColumn >= 0);
        for (LogicalPlan branch : union.children()) {
            Attribute branchKey = branch.output().get(keyColumn);
            TimeSeriesHash matchingKey = as(outputExpression(branch, branchKey), TimeSeriesHash.class);
            assertOnKey(matchingKey, "cluster");
            assertIndependentSources(branch, 1);
        }
        assertTrue(plan.collect(TopNBy.class).isEmpty());
        assertIndependentSources(plan, 2);
    }

    public void testNestedSetOperatorsComposeWithAggregationAndArithmetic() {
        for (String expression : List.of(
            "sum by(cluster)(network.bytes_in or ignoring(pod) network.total_bytes_out)",
            "(network.bytes_in or network.total_bytes_out) and on(cluster) network.bytes_in",
            "network.bytes_in or (network.total_bytes_out unless on(cluster) network.bytes_in)",
            "(sum by(cluster)(network.bytes_in) or sum by(cluster)(network.total_bytes_out)) + sum by(cluster)(network.bytes_in)",
            "(network.bytes_in or network.total_bytes_out) * 2"
        )) {
            analyze(expression);
        }
    }

    public void testUnionChainKeepsOneSourceSubtreePerOperand() {
        for (boolean alternateMatching : List.of(false, true)) {
            String expression = "network.bytes_in";
            int operands = 8;
            for (int i = 1; i < operands; i++) {
                String matching = alternateMatching && i % 2 == 0 ? "ignoring(pod)" : "on(cluster)";
                expression = "(" + expression + " or " + matching + " network.total_bytes_out)";
            }
            LogicalPlan plan = analyze(expression);
            assertEquals(operands, plan.collect(EsRelation.class).size());
            assertEquals(operands - 1, plan.collect(InlineJoin.class).size());
        }
    }

    public void testRawAndFiniteUnionKeepsDeclaredColumnTypes() {
        LogicalPlan plan = analyze("network.bytes_in or sum by(cluster)(network.total_bytes_out)");
        assertEquals(List.of("result", "step", "_timeseries", "cluster"), plan.output().stream().map(Attribute::name).toList());
        assertEquals(DataType.KEYWORD, plan.output().getLast().dataType());
        assertConsistent(
            prototype().query(
                query("network.bytes_in or sum by(cluster)(network.total_bytes_out)") + " | EVAL present = cluster IS NOT NULL"
            )
        );
    }

    public void testRawAndFiniteUnionPreservesNativeTypesOnEveryVersionPath() {
        for (TestAnalyzer analyzer : List.of(
            numericDimensions().flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(false)),
            numericDimensions().minimumTransportVersion(
                TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"))
            ),
            numericDimensions()
        )) {
            for (String expression : List.of(
                "network.bytes_in or sum by(shard)(network.total_bytes_out)",
                "sum by(shard)(network.total_bytes_out) or network.bytes_in"
            )) {
                LogicalPlan plan = analyzer.query(query(expression));
                assertConsistent(plan);
                assertEquals(
                    DataType.LONG,
                    plan.output().stream().filter(attribute -> attribute.name().equals("shard")).findFirst().orElseThrow().dataType()
                );
                assertConsistent(analyzer.query(query(expression) + " | EVAL next_shard = shard + 1"));
            }
        }
    }

    public void testUnionUsesRelabelTypeFactsThroughTransparentAndSetWrappers() {
        String relabel = "label_replace(network.bytes_in, \"shard\", \"x\", \"\", \"\")";
        for (String left : List.of(relabel, "abs(" + relabel + ")", "(" + relabel + " * 2)", "(" + relabel + " or network.bytes_in)")) {
            String expression = left + " or sum by(shard)(network.total_bytes_out)";
            LogicalPlan plan = numericDimensions().query(query(expression));
            assertConsistent(plan);
            assertEquals(
                DataType.KEYWORD,
                plan.output().stream().filter(attribute -> attribute.name().equals("shard")).findFirst().orElseThrow().dataType()
            );
            assertConsistent(numericDimensions().query(query(expression) + " | EVAL text = CONCAT(shard, \"!\")"));
        }
    }

    public void testGroupModifiersTakeCopiedColumnTypesFromTheOneSide() {
        String nativeLabels = "sum by(cluster, shard)(network.bytes_in)";
        String textLabels = "label_replace(sum by(cluster)(network.total_bytes_out), \"shard\", \"x\", \"\", \"\")";
        for (String expression : List.of(
            nativeLabels + " * on(cluster) group_left(shard) " + textLabels,
            textLabels + " * on(cluster) group_right(shard) " + nativeLabels
        )) {
            LogicalPlan plan = numericDimensions().query(query(expression) + " | EVAL text = CONCAT(shard, \"!\")");
            assertConsistent(plan);
            assertEquals(
                DataType.KEYWORD,
                plan.output().stream().filter(attribute -> attribute.name().equals("shard")).findFirst().orElseThrow().dataType()
            );
        }
        for (String expression : List.of(
            textLabels + " * on(cluster) group_left(shard) " + nativeLabels,
            nativeLabels + " * on(cluster) group_right(shard) " + textLabels
        )) {
            LogicalPlan plan = numericDimensions().query(query(expression) + " | EVAL next_shard = shard + 1");
            assertConsistent(plan);
            assertEquals(
                DataType.LONG,
                plan.output().stream().filter(attribute -> attribute.name().equals("shard")).findFirst().orElseThrow().dataType()
            );
        }
    }

    public void testDroppedMatchingLabelsDoNotLeakTheirTypeFacts() {
        String relabel = "label_replace(network.bytes_in, \"shard\", \"x\", \"\", \"\")";
        for (String matching : List.of("on(cluster)", "ignoring(shard)")) {
            String expression = "(" + relabel + " + " + matching + " network.total_bytes_out) or sum by(shard)(network.bytes_in)";
            LogicalPlan plan;
            try {
                plan = numericDimensions().query(query(expression) + " | EVAL next_shard = shard + 1");
            } catch (RuntimeException e) {
                throw new AssertionError(expression, e);
            }
            assertConsistent(plan);
            assertEquals(
                DataType.LONG,
                plan.output().stream().filter(attribute -> attribute.name().equals("shard")).findFirst().orElseThrow().dataType()
            );
        }
    }

    public void testUnionOutputIdentitiesAreIndependentOfBranchConstants() {
        for (String expression : List.of(
            "label_replace(vector(1), \"host\", \"a\", \"\", \"\") or network.bytes_in",
            "label_replace(sum(network.bytes_in), \"_timeseries\", \"x\", \"\", \"\") or network.total_bytes_out"
        )) {
            LogicalPlan plan = analyze(expression);
            for (UnionAll union : plan.collect(UnionAll.class)) {
                for (LogicalPlan branch : union.children()) {
                    for (Attribute output : union.output()) {
                        assertFalse(branch.output().stream().anyMatch(attribute -> attribute.id().equals(output.id())));
                    }
                }
            }
        }
    }

    /** A union that adds no label columns passes through each branch's residual metadata without decoding it again. */
    public void testUnionKeepsAlreadyDisjointMetadata() {
        for (String matching : List.of("", "on(cluster)")) {
            LogicalPlan plan = analyze("network.bytes_in or " + matching + " network.total_bytes_out");
            UnionAll union = plan.collect(UnionAll.class).getFirst();
            for (LogicalPlan branch : union.children()) {
                as(metadataOutputExpression(branch), Attribute.class);
            }
        }
    }

    public void testUnionPromotesLabelsFromEachBranchesResidualMetadata() {
        String relabel = "label_replace(network.bytes_in, \"zone\", \"x\", \"cluster\", \".*\")";
        for (boolean relabelFirst : List.of(false, true)) {
            String expression = relabelFirst ? relabel + " or network.total_bytes_out" : "network.total_bytes_out or " + relabel;
            LogicalPlan plan = analyze(expression);
            UnionAll union = plan.collect(UnionAll.class).getFirst();
            LogicalPlan rawBranch = union.children().get(relabelFirst ? 1 : 0);
            for (LogicalPlan branch : union.children()) {
                var gets = new ArrayList<TimeSeriesGet>();
                branch.output()
                    .stream()
                    .filter(attribute -> attribute.dataType() == DataType.KEYWORD)
                    .map(attribute -> outputExpression(branch, attribute))
                    .forEach(expressionValue -> expressionValue.forEachDown(TimeSeriesGet.class, gets::add));
                if (branch == rawBranch) {
                    assertTrue(gets.stream().anyMatch(get -> get.children().contains(Literal.keyword(get.source(), "cluster"))));
                    assertTrue(gets.stream().anyMatch(get -> get.children().contains(Literal.keyword(get.source(), "zone"))));
                }
                if (branch == rawBranch) {
                    TimeSeriesUnset unset = as(metadataOutputExpression(branch), TimeSeriesUnset.class);
                    assertEquals(
                        List.of(Literal.keyword(unset.source(), "cluster"), Literal.keyword(unset.source(), "zone")),
                        unset.children().subList(1, unset.children().size())
                    );
                } else {
                    // Relabel already promoted zone for its no-match fallback. The union must not add another
                    // rewrite for either zone or the cluster column that was promoted at the source.
                    TimeSeriesUnset unset = as(metadataOutputExpression(branch), TimeSeriesUnset.class);
                    assertEquals(List.of(Literal.keyword(unset.source(), "zone")), unset.children().subList(1, unset.children().size()));
                    as(unset.metadata(), Attribute.class);
                }
            }
        }
        assertConsistent(
            numericDimensions().query(
                query("sum without(shard)(network.bytes_in) or sum by(shard)(network.total_bytes_out)") + " | EVAL next_shard = shard + 1"
            )
        );
    }

    public void testMetadataAndLiteralTimeseriesLabelRemainDistinct() {
        String finite = "label_replace(sum(network.bytes_in), \"_timeseries\", \"x\", \"\", \"\")";
        for (String expression : List.of(finite + " or network.total_bytes_out", "network.total_bytes_out or " + finite)) {
            LogicalPlan plan = analyze(expression);
            assertEquals(List.of("result", "step", "_timeseries"), plan.output().stream().map(Attribute::name).toList());
            LogicalPlan grouped = analyze("sum by(_timeseries)(" + expression + ")");
            assertEquals(List.of("result", "step", "_timeseries"), grouped.output().stream().map(Attribute::name).toList());
            assertEquals(DataType.KEYWORD, grouped.output().getLast().dataType());
        }
    }

    public void testMatchingTimeseriesLabelDoesNotSelectTheMetadataCarrier() {
        for (String left : List.of("network.bytes_in", "label_replace(network.bytes_in, \"_timeseries\", \"x\", \"\", \"\")")) {
            LogicalPlan on = analyze(left + " + on(_timeseries) network.total_bytes_out");
            assertEquals(List.of("result", "step", "_timeseries"), on.output().stream().map(Attribute::name).toList());
            var renders = metadataRenders(on);
            assertTrue("ON retains only the requested ordinary label", renders.isEmpty());

            LogicalPlan ignoring = analyze(left + " + ignoring(_timeseries) network.total_bytes_out");
            assertEquals(List.of("result", "step", "_timeseries"), ignoring.output().stream().map(Attribute::name).toList());
            renders = metadataRenders(ignoring);
            assertFalse("IGNORING must preserve unrelated runtime labels", renders.isEmpty());
        }
    }

    public void testCopiedTimeseriesLabelDoesNotBecomeAMetadataCarrier() {
        String derived = "label_replace(sum by(cluster)(network.total_bytes_out), \"_timeseries\", \"x\", \"\", \"\")";
        for (String expression : List.of(
            "network.bytes_in * on(cluster) group_left(_timeseries) " + derived,
            derived + " * on(cluster) group_right(_timeseries) network.bytes_in"
        )) {
            var renders = metadataRenders(analyze(expression));
            assertFalse("the many side retains its metadata", renders.isEmpty());
        }
        for (String expression : List.of(
            "sum by(cluster)(network.bytes_in) * on(cluster) group_left(_timeseries) network.total_bytes_out",
            "network.total_bytes_out * on(cluster) group_right(_timeseries) sum by(cluster)(network.bytes_in)"
        )) {
            var renders = metadataRenders(analyze(expression));
            assertTrue("copying one label must not expose the one side's metadata", renders.isEmpty());
        }
    }

    public void testAbsentCopiedTimeseriesLabelDoesNotUseTheMetadataType() {
        String one = "sum by(cluster)(network.total_bytes_out)";
        for (String many : List.of("network.bytes_in", "label_replace(network.bytes_in, \"_timeseries\", \"x\", \"\", \"\")")) {
            for (String expression : List.of(
                many + " * on(cluster) group_left(_timeseries) " + one,
                one + " * on(cluster) group_right(_timeseries) " + many
            )) {
                var renders = metadataRenders(analyze(expression));
                assertFalse("the absent copied label must not replace the many side's metadata", renders.isEmpty());
            }
        }
    }

    public void testDefaultMatchingExcludesMetricNameButOnCanMatchIt() {
        LogicalPlan defaultPlan = analyze("network.bytes_in and network.total_bytes_out");
        for (TimeSeriesHash hash : membershipKeys(defaultPlan)) {
            TimeSeriesUnset metadata = as(hash.metadata(), TimeSeriesUnset.class);
            assertTrue(metadata.children().contains(Literal.keyword(metadata.source(), LabelMatcher.NAME)));
        }
        assertIndependentSources(defaultPlan, 2);

        LogicalPlan onPlan = analyze("network.bytes_in and on(__name__) network.total_bytes_out");
        for (TimeSeriesHash hash : membershipKeys(onPlan)) {
            assertOnKey(hash, LabelMatcher.NAME);
        }
        assertIndependentSources(onPlan, 2);
    }

    public void testEmptyMatchingLabelsUseNonNullCanonicalKeys() {
        LogicalPlan plan = analyze("network.bytes_in unless on(missing) network.total_bytes_out");
        List<TimeSeriesHash> hashes = membershipKeys(plan);
        assertEquals(2, hashes.size());
        // Missing/empty labels are normalized by TS_HASH; SQL NULL must never reach the anti-join key.
        for (TimeSeriesHash hash : hashes) {
            assertEquals(DataType.TSID_DATA_TYPE, hash.dataType());
            assertOnKey(hash, "missing");
        }
        assertIndependentSources(plan, 2);
    }

    public void testHistogramWithoutBucketsKeepsConstantStepGrid() {
        for (String histogram : List.of("histogram_quantile(0.5, vector(7))", "histogram_fraction(0, 1, vector(7))")) {
            for (String expression : List.of(histogram, histogram + " or vector(2)")) {
                LogicalPlan plan = prototype().query(
                    "PROMQL start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(" + expression + ") | LIMIT 100"
                );
                assertConsistent(plan);
                assertEquals(List.of(DataType.DOUBLE, DataType.DATETIME), plan.output().stream().map(Attribute::dataType).toList());
                assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
                assertTrue(expression, plan.collect(TimeSeriesAggregate.class).isEmpty());
            }
        }
    }

    public void testConstantVectorsUseVectorMatchingWhenEnabled() {
        for (String expression : List.of("network.bytes_in + vector(1)", "vector(1) + network.bytes_in")) {
            LogicalPlan plan = analyze(expression);
            assertEquals(1, plan.collect(InnerJoin.class).size());
            var hashes = new ArrayList<TimeSeriesHash>();
            plan.forEachExpressionDown(TimeSeriesHash.class, hashes::add);
            assertFalse(hashes.isEmpty());
        }
        for (String expression : List.of(
            "network.bytes_in or vector(0)",
            "vector(0) and on() network.bytes_in",
            "vector(1) or vector(2)"
        )) {
            analyze(expression);
        }
    }

    public void testEmptySourceCanStillReturnAConstantUnionOperand() {
        for (String expression : List.of("foo or vector(0)", "foo and vector(0)", "vector(0) unless foo")) {
            LogicalPlan plan = analyzer().addEmptyIndex()
                .unmappedResolution(UnmappedResolution.NULLIFY)
                .minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
                .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true))
                .query(
                    "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:01:00Z\" step=1m result=("
                        + expression
                        + ") | LIMIT 100"
                );
            assertConsistent(plan);
            assertTrue(plan.collect(EsRelation.class).isEmpty());
        }
    }

    public void testScalarOperandsRemainInvalid() {
        for (String operator : List.of("and", "or", "unless")) {
            var error = expectThrows(VerificationException.class, () -> prototype().query(query("network.bytes_in " + operator + " 1")));
            assertThat(error.getMessage(), containsString("not allowed in binary scalar expression"));
        }
    }

    public void testDisabledAndOldVersionsKeepLegacyRestrictions() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(false)),
            prototype().minimumTransportVersion(
                TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"))
            )
        )) {
            for (String expression : List.of(
                "network.bytes_in and network.total_bytes_out",
                "network.bytes_in unless network.total_bytes_out",
                "network.bytes_in or on(cluster) network.total_bytes_out"
            )) {
                expectThrows(VerificationException.class, () -> analyzer.query(query(expression)));
            }
            LogicalPlan plan = analyzer.query(query("network.bytes_in or network.total_bytes_out"));
            plan.forEachExpressionDown(TimeSeriesFunction.class, function -> fail("legacy union must not use typed metadata"));
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().addK8s()
            .minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TestAnalyzer numericDimensions() {
        var mapping = new HashMap<>(loadMapping("k8s-mappings.json"));
        mapping.put("shard", new EsField("shard", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
        return prototype().addIndex(
            new EsIndex(
                "k8s",
                mapping,
                Map.of("k8s", new IndexProperties(IndexMode.TIME_SERIES, 0)),
                Map.of("", List.of("k8s")),
                Map.of("", List.of("k8s"))
            )
        );
    }

    private LogicalPlan analyze(String expression) {
        LogicalPlan plan = prototype().query(query(expression));
        assertConsistent(plan);
        return plan;
    }

    private static List<ToString> metadataRenders(LogicalPlan plan) {
        var renders = new ArrayList<ToString>();
        plan.forEachExpressionDown(ToString.class, render -> {
            if (render.field().dataType() == DataType.TIMESERIES_DATA_TYPE) {
                renders.add(render);
            }
        });
        return renders;
    }

    /** Resolve the actual branch output by identity; analysis may move or remove its proxy alias. */
    private static Expression metadataOutputExpression(LogicalPlan branch) {
        Attribute metadata = branch.output()
            .stream()
            .filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE)
            .findFirst()
            .orElseThrow();
        return outputExpression(branch, metadata);
    }

    private static Expression outputExpression(LogicalPlan plan, Attribute output) {
        var definitions = new HashMap<NameId, Expression>();
        plan.forEachExpressionDown(Alias.class, alias -> definitions.put(alias.id(), alias.child()));
        Expression expression = output;
        while (expression instanceof Attribute attribute && definitions.containsKey(attribute.id())) {
            expression = definitions.remove(attribute.id());
        }
        return expression;
    }

    private static List<TimeSeriesHash> membershipKeys(LogicalPlan plan) {
        List<AbstractSubqueryJoin> joins = plan.collect(AbstractSubqueryJoin.class);
        assertEquals(1, joins.size());
        AbstractSubqueryJoin join = joins.getFirst();
        assertEquals(2, join.config().leftFields().size());
        assertEquals(2, join.config().rightFields().size());
        return List.of(
            as(outputExpression(join.left(), join.config().leftFields().getLast()), TimeSeriesHash.class),
            as(outputExpression(join.right(), join.config().rightFields().getLast()), TimeSeriesHash.class)
        );
    }

    private static void assertOnKey(TimeSeriesHash hash, String label) {
        Literal metadata = as(hash.metadata(), Literal.class);
        assertEquals(DataType.TIMESERIES_DATA_TYPE, metadata.dataType());
        assertNull(metadata.value());
        assertEquals(3, hash.children().size());
        assertEquals(Literal.keyword(hash.source(), label), hash.children().get(1));
    }

    private static void assertIndependentSources(LogicalPlan plan, int sources) {
        assertEquals(sources, plan.collect(EsRelation.class).size());
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertEquals(1, relation.output().stream().filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE).count());
        }
    }

    private static String query(String expression) {
        return "PROMQL index=k8s start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" step=1h result=("
            + expression
            + ") | LIMIT 100";
    }

    private static void assertConsistent(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
        plan.forEachUp(
            node -> assertTrue(
                node.output().stream().filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE).count() <= 1
            )
        );
    }
}
