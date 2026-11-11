/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/** How PromQL finds a label among the fields of its source and a node's declared output, and what its tags label leaves out. */
public class PromqlLabelsTests extends ESTestCase {

    private final FieldAttribute pod = field("pod", EsField.TimeSeriesFieldType.DIMENSION);
    private final FieldAttribute event = field("event", EsField.TimeSeriesFieldType.NONE);
    private final EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(pod, event));

    /** Any field of a series is one of its labels, but only a dimension identifies the series. */
    public void testFieldIsALabelDimensionOrNot() {
        assertThat(PromqlLabels.getField(relation, "event"), sameInstance(event));
        assertThat(PromqlLabels.getLabel(relation, "event"), nullValue());
        assertThat(PromqlLabels.getLabel(relation, "pod"), sameInstance(pod));
        assertThat(PromqlLabels.getField(relation, "missing"), nullValue());
    }

    /** The tags label stands for every label a node does not declare by name. */
    public void testTagsLabelAmongDeclaredLabels() {
        UnresolvedPromqlLabelAttribute tags = UnresolvedPromqlLabelAttribute.newTagsLabel(Source.EMPTY);
        UnresolvedPromqlLabelAttribute cluster = UnresolvedPromqlLabelAttribute.label(Source.EMPTY, "cluster");
        List<Attribute> declared = List.of(cluster, tags);
        assertThat(PromqlLabels.getTagsLabel(declared), sameInstance(tags));
        assertTrue(PromqlLabels.isTagsLabel(tags));
        assertFalse(PromqlLabels.isTagsLabel(cluster));
        assertThat(PromqlLabels.getTagsLabel(List.of(cluster)), nullValue());
    }

    private static FieldAttribute field(String name, EsField.TimeSeriesFieldType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, DataType.KEYWORD, Map.of(), true, type));
    }

    /** A label excluded in only one branch is still available in the union; branch order cannot change that. */
    public void testUnionExclusionsMatchDeclaredOutput() {
        for (int left = 0; left < 8; left++) {
            for (int right = 0; right < 8; right++) {
                LogicalPlan union = new VectorBinarySet(
                    Source.EMPTY,
                    input(names(left)),
                    input(names(right)),
                    VectorMatch.NONE,
                    VectorBinarySet.SetOp.UNION
                );
                Set<String> expected = new HashSet<>(names(left));
                expected.retainAll(names(right));
                expected.add(LabelMatcher.NAME);
                assertEquals(expected, PromqlLabels.excluded(union));
            }
        }
    }

    /** Histograms add their own removals without restoring labels their child removed. */
    public void testHistogramExclusionsCompose() {
        for (int inherited = 0; inherited < 8; inherited++) {
            for (LogicalPlan histogram : histograms(input(names(inherited)))) {
                Set<String> expected = new HashSet<>(names(inherited));
                expected.addAll(Set.of("le", LabelMatcher.NAME));
                assertEquals(expected, PromqlLabels.excluded(histogram));
            }
        }
    }

    /**
     * A histogram over promoted labels keeps the metric name: translation groups buckets by this output, and dropping the
     * name before the quantile would merge histogram families.
     */
    public void testHistogramKeepsPromotedMetricName() {
        LogicalPlan promoted = new Project(
            Source.EMPTY,
            EsqlTestUtils.relation(IndexMode.TIME_SERIES),
            List.of(
                new ReferenceAttribute(Source.EMPTY, "pod", DataType.KEYWORD),
                new ReferenceAttribute(Source.EMPTY, "le", DataType.KEYWORD),
                new ReferenceAttribute(Source.EMPTY, "__name__", DataType.KEYWORD)
            )
        );
        for (LogicalPlan histogram : histograms(promoted)) {
            assertEquals(List.of("pod", "__name__"), histogram.output().stream().map(Attribute::name).toList());
            assertNull(PromqlLabels.excluded(histogram));
        }
    }

    private static List<LogicalPlan> histograms(LogicalPlan input) {
        return List.of(
            new HistogramQuantile(
                Source.EMPTY,
                input,
                PromqlHistogramQuantile.PROMQL_DEFINITION,
                List.of(new Literal(Source.EMPTY, 0.5, DataType.DOUBLE))
            ),
            new HistogramFraction(
                Source.EMPTY,
                input,
                org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction.PROMQL_DEFINITION,
                List.of(new Literal(Source.EMPTY, 0.0, DataType.DOUBLE), new Literal(Source.EMPTY, 1.0, DataType.DOUBLE))
            )
        );
    }

    /** Series that leave out {@code exclusions} and the metric name: a without over series with every label in their tags label. */
    private static LogicalPlan input(Set<String> exclusions) {
        LogicalPlan series = new Project(
            Source.EMPTY,
            EsqlTestUtils.relation(IndexMode.TIME_SERIES),
            List.of(UnresolvedPromqlLabelAttribute.newTagsLabel(Source.EMPTY))
        );
        List<Attribute> without = exclusions.stream()
            .<Attribute>map(name -> UnresolvedPromqlLabelAttribute.label(Source.EMPTY, name))
            .toList();
        return new AcrossSeriesAggregate(
            Source.EMPTY,
            series,
            Sum.PROMQL_DEFINITION,
            List.of(),
            AcrossSeriesAggregate.Grouping.WITHOUT,
            without
        );
    }

    private static Set<String> names(int mask) {
        List<String> labels = List.of("pod", "region", "cluster");
        var names = new HashSet<String>();
        for (int bit = 0; bit < labels.size(); bit++) {
            if ((mask & (1 << bit)) != 0) {
                names.add(labels.get(bit));
            }
        }
        return names;
    }
}
