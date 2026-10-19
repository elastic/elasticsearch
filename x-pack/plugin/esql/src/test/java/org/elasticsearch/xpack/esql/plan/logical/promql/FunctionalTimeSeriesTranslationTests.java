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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.LabelEval;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesSet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Verifies the exclusive label representations of the admitted aggregation translation path. */
public class FunctionalTimeSeriesTranslationTests extends ESTestCase {
    /** These plan-shape assertions describe the opt-in representation, not the existing translation. */
    @Before
    public void requireFunctionalTimeSeries() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
    }

    /** Only public output columns constrain the result name, not labels retained inside the metadata dictionary. */
    public void testResultNameCannotDuplicateAnOutputColumn() {
        for (String name : List.of("cluster", "_timeseries", "step")) {
            String expression = name.equals("cluster") ? "sum by(cluster)(network.cost)" : "network.cost";
            VerificationException error = expectThrows(
                VerificationException.class,
                () -> prototype().query("PROMQL index=k8s step=1h `" + name + "`=(" + expression + ") | LIMIT 10")
            );
            assertThat(
                error.getMessage(),
                containsString(
                    "Result column name [" + name + "] is already used by another output column; choose a different result column name"
                )
            );
        }

        LogicalPlan raw = prototype().query("PROMQL index=k8s step=1h cluster=(network.cost) | LIMIT 10");
        assertEquals(List.of("cluster", "step", "_timeseries"), raw.output().stream().map(Attribute::name).toList());
        assertConsistent(raw);

        LogicalPlan removedLabel = prototype().query(
            "PROMQL index=k8s step=1h cluster=(sum without(cluster)(sum by(cluster,pod)(network.cost))) | LIMIT 10"
        );
        assertEquals(List.of("cluster", "step", "pod"), removedLabel.output().stream().map(Attribute::name).toList());
        assertConsistent(removedLabel);
    }

    /** Named label columns do not require participating nodes to understand the new metadata type. */
    public void testNamedAggregationWorksWithOlderParticipatingNode() {
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
        assertNamedAggregationsDoNotReadMetadata(prototype().minimumTransportVersion(previous));
    }

    /** Non-aggregation nodes are rejected even when they are nested underneath a supported aggregate. */
    public void testMixedTreesAreRejected() {
        for (String expression : List.of(
            "sum(network.cost) + sum(network.cost)",
            "sum(network.cost) or sum(network.cost)",
            "sum(network.cost) and sum(network.cost)",
            "sum(network.cost) unless sum(network.cost)",
            "sum(abs(network.cost))",
            "scalar(sum(network.cost))",
            "sum(vector(1))",
            "sum(hour(network.cost))",
            "histogram_quantile(0.5, sum by(cluster)(network.cost))",
            "sort(sum(network.cost))",
            "topk(1, network.cost)",
            "sum by(cluster)(label_join(network.cost, \"cluster\", \"/\", \"pod\"))"
        )) {
            VerificationException error = expectThrows(VerificationException.class, () -> prototype().query(query(expression)));
            assertThat(
                expression,
                error.getMessage(),
                containsString("supports only metric selectors, per-series range functions, and aggregations")
            );
        }
    }

    /** Named grouping and regrouping never need a metadata reader or metadata scalar functions. */
    public void testNamedAggregationDoesNotReadMetadata() {
        assertNamedAggregationsDoNotReadMetadata(prototype());
    }

    private static void assertNamedAggregationsDoNotReadMetadata(TestAnalyzer analyzer) {
        for (String expression : List.of(
            "sum by(cluster,pod)(network.cost)",
            "sum by(cluster)(avg_over_time(network.cost[5m]))",
            "max without(pod)(sum by(cluster,pod)(network.cost))",
            "sum by(cluster)(max without(pod)(sum by(cluster,region,pod)(network.cost)))",
            "sum(network.cost)"
        )) {
            LogicalPlan plan = analyzer.query(query(expression));
            assertTrue(expression, metadataReaders(plan).isEmpty());
            assertTrue(expression, expressions(plan, TimeSeriesFunction.class).isEmpty());
            assertTrue(expression, expressions(plan, LabelEval.class).isEmpty());
            assertFalse(
                expression,
                expressions(plan, Expression.class).stream().anyMatch(e -> e.dataType() == DataType.TIMESERIES_DATA_TYPE)
            );
            assertConsistent(plan);
        }
    }

    /** Promoted source labels and nested BY/WITHOUT boundaries retain each complete multivalued dimension. */
    public void testNamedAggregationsRetainCompleteLabelValues() {
        for (String expression : List.of(
            "sum by(cluster,region,pod)(network.cost)",
            "sum by(cluster,region)(sum by(cluster,region,pod)(network.cost))",
            "sum without(pod)(sum by(cluster,region,pod)(network.cost))",
            "sum by(cluster)(sum without(region)(sum by(cluster,region,pod)(network.cost)))"
        )) {
            LogicalPlan plan = prototype().query(query(expression));
            assertTrue(expression, expressions(plan, PackDimsAgg.class).isEmpty());
            assertFalse(expression, expressions(plan, DimensionValues.class).isEmpty());
            assertFalse(expression, plan.collect(UnpackDims.class).isEmpty());
            for (Aggregate aggregate : plan.collect(Aggregate.class)) {
                assertFalse(
                    expression + ": " + aggregate,
                    aggregate.groupings().stream().anyMatch(group -> group.dataType() == DataType.KEYWORD)
                );
            }
            assertConsistent(plan);
        }
    }

    /** Empty values disappear without applying a scalar comparison to an array of label values. */
    public void testPromotedLabelsNormalizeEmptyValues() {
        LogicalPlan plan = prototype().query(query("sum by(region)(network.cost)"));
        List<MvDifference> normalizations = expressions(plan, MvDifference.class);
        assertEquals(plan.toString(), 1, normalizations.size());
        MvDifference normalization = normalizations.getFirst();
        BytesRef empty = new BytesRef("");
        BytesRef eu = new BytesRef("eu");
        BytesRef us = new BytesRef("us");
        assertNull(normalizeLabel(normalization, null));
        assertNull(normalizeLabel(normalization, empty));
        assertEquals(eu, normalizeLabel(normalization, eu));
        assertNull(normalizeLabel(normalization, List.of(empty)));
        assertEquals(eu, normalizeLabel(normalization, List.of(empty, eu)));
        assertEquals(List.of(eu, us), normalizeLabel(normalization, List.of(empty, eu, us)));
        assertConsistent(plan);
    }

    private static Object normalizeLabel(MvDifference normalization, Object value) {
        return normalization.replaceChildren(List.of(new Literal(normalization.source(), value, DataType.KEYWORD), normalization.right()))
            .fold(FoldContext.small());
    }

    /** Open aggregation reads one complete identity; each WITHOUT changes it after its child has evaluated. */
    public void testDerivedAggregationReadsOneCompleteMetadataValue() {
        var cases = Map.of(
            "network.cost",
            Set.<String>of(),
            "avg_over_time(network.cost[5m])",
            Set.of(LabelMatcher.NAME),
            "sum without(pod)(network.cost)",
            Set.of("pod", LabelMatcher.NAME),
            "sum without(cluster)(max without(pod)(network.cost))",
            Set.of("pod", LabelMatcher.NAME),
            "avg without(cluster)(sum without(region)(max without(pod)(network.cost)))",
            Set.of("pod", LabelMatcher.NAME),
            "avg by(cluster)(sum without()(network.cost))",
            Set.of(LabelMatcher.NAME)
        );
        for (var test : cases.entrySet()) {
            String expression = test.getKey();
            LogicalPlan plan = prototype().query(query(expression));
            List<TimeSeriesMetadataAttribute> readers = metadataReaders(plan);
            assertEquals(expression, 1, readers.size());
            assertEquals(expression, test.getValue(), readers.getFirst().excludedFields());
            assertCompleteMetadata(plan);
        }
    }

    /** BY reads the current derived labels after WITHOUT; it cannot recover a removed label from the source. */
    public void testByReadsTheCurrentWithoutResult() {
        LogicalPlan plan = prototype().query(query("sum by(pod)(sum without(pod)(network.cost))"));
        List<LabelEval> reads = expressions(plan, LabelEval.class);
        assertEquals(1, reads.size());
        Attribute metadata = (Attribute) reads.getFirst().children().getFirst();
        assertNotEquals(metadataReaders(plan).getFirst().id(), metadata.id());
        assertEquals(Set.of("pod", LabelMatcher.NAME), metadataReaders(plan).getFirst().excludedFields());
        assertTrue(metadataRenders(plan).isEmpty());
        assertEquals(List.of("result", "step", "pod"), plan.output().stream().map(Attribute::name).toList());
        assertCompleteMetadata(plan);
    }

    /** Range aggregation remains a per-series operation below the nonlinear cross-series grouping stages. */
    public void testWithinSeriesFunctionFeedsNestedWithout() {
        LogicalPlan plan = prototype().query(query("max without(cluster)(sum without(pod)(avg_over_time(network.cost[5m])))"));
        assertEquals(1, expressions(plan, Avg.class).size());
        assertEquals(1, metadataReaders(plan).size());
        assertFalse(expressions(plan, TimeSeriesUnset.class).isEmpty());
        assertCompleteMetadata(plan);
    }

    /** Source predicates precede label removal, and each leaf defines its shifted evaluation clock only once. */
    public void testSelectorFiltersAndEvaluationClockAreAtTheLeaf() {
        for (DataType timestampType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            for (boolean bounded : List.of(false, true)) {
                String bounds = bounded ? "start=\"2024-05-10T00:01:00Z\" end=\"2024-05-10T00:02:00Z\" " : "";
                LogicalPlan plan = nativeLabels(timestampType).query(
                    "PROMQL index=typed_labels "
                        + bounds
                        + "step=1m result=(sum without(host)(avg_over_time(tx{host=\"a\"}[2m] offset 1m))) | LIMIT 10"
                );
                List<Filter> sourceFilters = plan.collect(Filter.class)
                    .stream()
                    .filter(filter -> filter.child() instanceof EsRelation)
                    .toList();
                assertEquals(plan.toString(), 1, sourceFilters.size());
                List<String> filteredFields = sourceFilters.getFirst().condition().references().stream().map(Attribute::name).toList();
                assertTrue(filteredFields.toString(), filteredFields.contains("host"));
                assertEquals(filteredFields.toString(), bounded, filteredFields.contains("@timestamp"));
                assertEquals(plan.toString(), 1, expressions(plan, Add.class).size());
                assertEquals(plan.toString(), timestampType == DataType.DATE_NANOS ? 1 : 0, expressions(plan, ToDatetime.class).size());
                assertCompleteMetadata(plan);
            }
        }
    }

    /** Public output references the value conversion and metadata rendering, each materialized exactly once. */
    public void testFinalValueAndMetadataAreMaterializedOnce() {
        LogicalPlan plan = prototype().query(query("sum without(pod)(network.bytes_in)"));
        List<Eval> renders = plan.collect(Eval.class)
            .stream()
            .filter(eval -> eval.fields().stream().anyMatch(alias -> isMetadataRender(alias.child())))
            .toList();
        assertEquals(1, renders.size());
        Eval outputEval = renders.getFirst();
        Alias value = plan.collect(Eval.class)
            .stream()
            .flatMap(eval -> eval.fields().stream())
            .filter(alias -> alias.child() instanceof ToDouble)
            .findFirst()
            .orElseThrow();
        Alias metadata = outputEval.fields().stream().filter(alias -> isMetadataRender(alias.child())).findFirst().orElseThrow();
        Project project = plan.collect(Project.class)
            .stream()
            .filter(candidate -> candidate.child() == outputEval)
            .findFirst()
            .orElseThrow();
        List<Attribute> projected = project.projections().stream().map(expression -> {
            Expression reference = expression instanceof Alias alias ? alias.child() : expression;
            return asInstanceOf(Attribute.class, reference);
        }).toList();
        assertTrue(projected.stream().anyMatch(attribute -> attribute.id().equals(value.id())));
        assertTrue(projected.stream().anyMatch(attribute -> attribute.id().equals(metadata.id())));
        assertEquals(List.of("result", "step", "_timeseries"), project.output().stream().map(Attribute::name).toList());
        assertEquals(1, expressions(plan, ToDouble.class).size());
        assertCompleteMetadata(plan);
    }

    /** Deriving labels from metadata must preserve their declared native types when BY exposes them as columns. */
    public void testByOverWithoutPreservesNativeLabelTypes() {
        for (String expression : List.of(
            "sum by(shard,enabled,address)(tx)",
            "sum by(shard,enabled,address)(sum without(host)(tx))",
            "sum by(shard,enabled,address)(sum without(shard,enabled,address)(tx))"
        )) {
            LogicalPlan plan = nativeLabels().query(nativeQuery(expression));
            assertNativeLabelTypes(plan);
            if (expression.contains("without")) {
                assertEquals(3, expressions(plan, LabelEval.class).size());
                assertCompleteMetadata(plan);
            } else {
                assertTrue(metadataReaders(plan).isEmpty());
                assertConsistent(plan);
            }
        }
    }

    /** A label absent from a finite child becomes a null of its declared type, even if its source field remains in the plan. */
    public void testMissingNamedLabelsUseTypedNulls() {
        LogicalPlan plan = nativeLabels().query(nativeQuery("sum by(shard,enabled,address)(sum by(host)(tx))"));
        assertNativeLabelTypes(plan);
        assertTrue(metadataReaders(plan).isEmpty());
        assertTrue(expressions(plan, LabelEval.class).isEmpty());
        for (DataType type : List.of(DataType.LONG, DataType.BOOLEAN, DataType.IP)) {
            assertTrue(
                "Missing null grouping of type " + type + " in " + plan,
                plan.collect(Eval.class)
                    .stream()
                    .flatMap(eval -> eval.fields().stream())
                    .anyMatch(alias -> alias.child() instanceof Literal literal && literal.value() == null && literal.dataType() == type)
            );
        }
        assertConsistent(plan);
    }

    /** Empty source relations retain resolved output declarations without manufacturing metadata readers. */
    public void testEmptySourcesRetainConsistentAggregationOutput() {
        for (String expression : List.of("foo", "sum without(host)(foo)", "avg by(host)(sum without()(foo))")) {
            LogicalPlan plan = prototype().addEmptyIndex()
                .unmappedResolution(UnmappedResolution.NULLIFY)
                .query("PROMQL index=empty_index step=5m result=(" + expression + ") | LIMIT 10");
            assertTrue(plan.output().stream().allMatch(Attribute::resolved));
            assertTrue(plan.collect(EsRelation.class).isEmpty());
            assertFalse(plan.collect(LocalRelation.class).isEmpty());
            for (LocalRelation relation : plan.collect(LocalRelation.class)) {
                assertSame(EmptyLocalSupplier.EMPTY, relation.supplier());
            }
            assertConsistent(plan);
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().addK8s().minimumTransportVersion(TransportVersion.current());
    }

    private static TestAnalyzer nativeLabels() {
        return nativeLabels(DataType.DATETIME);
    }

    private static TestAnalyzer nativeLabels(DataType timestampType) {
        var mapping = new LinkedHashMap<String, EsField>();
        mapping.put("@timestamp", new EsField("@timestamp", timestampType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("tx", new EsField("tx", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
        var dimensions = Map.of("host", DataType.KEYWORD, "shard", DataType.LONG, "enabled", DataType.BOOLEAN, "address", DataType.IP);
        for (var field : dimensions.entrySet()) {
            mapping.put(
                field.getKey(),
                new EsField(field.getKey(), field.getValue(), Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
            );
        }
        var index = new EsIndex(
            "typed_labels",
            mapping,
            Map.of("typed_labels", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        return prototype().addIndex(index);
    }

    private static String query(String expression) {
        return "PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10";
    }

    private static String nativeQuery(String expression) {
        return "PROMQL index=typed_labels step=1h result=(" + expression + ") | LIMIT 10";
    }

    private static void assertNativeLabelTypes(LogicalPlan plan) {
        var actual = new LinkedHashMap<String, DataType>();
        plan.output().forEach(attribute -> actual.put(attribute.name(), attribute.dataType()));
        assertEquals(
            Map.of(
                "result",
                DataType.DOUBLE,
                "step",
                DataType.DATETIME,
                "shard",
                DataType.LONG,
                "enabled",
                DataType.BOOLEAN,
                "address",
                DataType.IP
            ),
            actual
        );
    }

    private static List<TimeSeriesMetadataAttribute> metadataReaders(LogicalPlan plan) {
        return plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .toList();
    }

    private static <T extends Expression> List<T> expressions(LogicalPlan plan, Class<T> type) {
        var expressions = new ArrayList<T>();
        plan.forEachExpressionDown(type, expressions::add);
        return expressions;
    }

    private static boolean isMetadataRender(Expression expression) {
        return expression instanceof ToString render && render.field().dataType() == DataType.TIMESERIES_DATA_TYPE;
    }

    private static List<ToString> metadataRenders(LogicalPlan plan) {
        return expressions(plan, ToString.class).stream().filter(FunctionalTimeSeriesTranslationTests::isMetadataRender).toList();
    }

    private static void assertCompleteMetadata(LogicalPlan plan) {
        assertConsistent(plan);
        assertTrue(expressions(plan, TimeSeriesSet.class).isEmpty());
        assertTrue(expressions(plan, TimeSeriesHash.class).isEmpty());
        plan.forEachDown(
            node -> assertTrue(
                node.toString(),
                node.output().stream().filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE).count() <= 1
            )
        );
        for (TimeSeriesFunction function : expressions(plan, TimeSeriesFunction.class)) {
            assertTrue(function.typeResolved().toString(), function.typeResolved().resolved());
            assertEquals(DataType.TIMESERIES_DATA_TYPE, function.children().getFirst().dataType());
        }
        for (LabelEval function : expressions(plan, LabelEval.class)) {
            assertTrue(function.typeResolved().toString(), function.typeResolved().resolved());
            assertEquals(DataType.TIMESERIES_DATA_TYPE, function.children().getFirst().dataType());
        }
        for (ToString render : metadataRenders(plan)) {
            assertTrue(render.typeResolved().toString(), render.typeResolved().resolved());
            assertEquals("No named-label overlay belongs in a complete metadata identity", 1, render.children().size());
        }
    }

    private static void assertConsistent(LogicalPlan plan) {
        var failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
    }
}
