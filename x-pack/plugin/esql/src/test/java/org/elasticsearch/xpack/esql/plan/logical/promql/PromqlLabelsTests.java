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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The labels a PromQL node's residual metadata leaves out, as its output declares them. */
public class PromqlLabelsTests extends ESTestCase {

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
                assertEquals(expected, PromqlLabels.metadataLabel(union.output()).unset());
                assertEquals(expected, PromqlLabels.excluded(union));
            }
        }
    }

    /** Histograms add their own removals without restoring labels their child removed. */
    public void testHistogramExclusionsCompose() {
        for (int inherited = 0; inherited < 8; inherited++) {
            for (LogicalPlan histogram : histograms(input(names(inherited)))) {
                Set<String> expected = new HashSet<>(names(inherited));
                expected.addAll(Set.of("le", "__name__"));
                assertEquals(expected, PromqlLabels.metadataLabel(histogram.output()).unset());
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

    /** A label name may contain the separator, so leaving out {@code a$b} differs from leaving out {@code a} and {@code b}. */
    public void testMetadataViewNamesAreDistinct() {
        List<Set<String>> views = List.of(
            Set.of(),
            Set.of("a"),
            Set.of("b"),
            Set.of("a", "b"),
            Set.of("a$b"),
            Set.of("a\\"),
            Set.of("a\\", "b"),
            Set.of("a\\$b"),
            Set.of("$"),
            Set.of("$", "a")
        );
        var names = new HashSet<String>();
        for (Set<String> view : views) {
            assertTrue(view.toString(), names.add(PromqlLabels.metadataLabelName(view)));
        }
        assertEquals("_timeseries$pod", PromqlLabels.metadataLabelName(Set.of("pod")));
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

    private static LogicalPlan input(Set<String> exclusions) {
        var labels = new ArrayList<Attribute>();
        labels.add(UnresolvedPromqlLabelRef.metadata(Source.EMPTY, exclusions));
        return new Project(Source.EMPTY, EsqlTestUtils.relation(IndexMode.TIME_SERIES), labels);
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
