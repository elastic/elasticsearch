/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/** Selectors translated over a label container. */
public class PromqlPlanSelectorV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanSelectorV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** An instant selector starts each series from the source's whole label container; its matchers filter the source. */
    public void testInstantSelectorReadsTheSourceLabelContainer() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h network.bytes_in{cluster=\"prod\"}", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        List<ToTimeSeriesMetadata> containers = expressions(collapse, ToTimeSeriesMetadata.class);
        assertThat(containers, hasSize(1));
        TimeSeriesWithout source = as(containers.getFirst().field(), TimeSeriesWithout.class);
        assertThat(source.children(), empty());

        Filter matchers = as(collapse.child(), Filter.class);
        assertThat(matchers.condition(), instanceOf(Equals.class));
        assertThat(matchers.child(), instanceOf(EsRelation.class));
        assertThat(plan.output().stream().map(a -> a.name()).toList(), hasItem("_timeseries"));
    }

    /** The translated selector plans through the optimizer. */
    public void testInstantSelectorOptimizes() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h network.bytes_in{cluster=\"prod\"}", true);
        assertThat(plan.collect(TimeSeriesAggregate.class), hasSize(1));
    }

    /** A literal is a vector without labels. */
    public void testLiteralSelectorHasNoLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h 1", false);
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), empty());
    }

    /** A node without a translation over the label container fails rather than mixing the two translations. */
    public void testNodeWithoutLabelTranslationFails() {
        var e = expectThrows(QlIllegalArgumentException.class, () -> planPromqlV2("PROMQL index=k8s step=1h sum(network.bytes_in)", false));
        assertThat(e.getMessage(), containsString("does not translate over a label container yet"));
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        var found = new ArrayList<E>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }
}
