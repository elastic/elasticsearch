/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Surrogate functions inside {@code NESTED_ANY} have to be rewritten the same way they are in a plain {@code WHERE}.
 */
public class NestedAnySurrogateTests extends AnalyzerTestCase {
    public NestedAnySurrogateTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testToIpInsideNestedAnyIsRewritten() {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        TransportVersion version = minimumVersionAtLeast(NestedPredicate.VERSION);
        var analyzer = AnalyzerNestedExpandTests.nestedAnalyzer(version);
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), version)
        );

        LogicalPlan control = optimizer.optimize(analyzer.query("FROM ne | WHERE TO_IP(id) IS NOT NULL"));
        assertThat(expressions(control, ToIp.class), empty());
        assertThat(expressions(control, ToIpLeadingZerosRejected.class), not(empty()));

        LogicalPlan nested = optimizer.optimize(analyzer.query("FROM ne | WHERE NESTED_ANY(TO_IP(items.sku) IS NOT NULL)"));
        List<NestedPredicate> found = new ArrayList<>();
        nested.forEachExpressionDown(NestedPredicate.class, found::add);
        assertThat(found, not(empty()));
        NestedPredicate predicate = found.getFirst();
        assertThat(predicate.children(), equalTo(List.of(predicate.body())));
        assertThat(predicate.references().isEmpty(), is(true));
        assertThat(expressions(predicate.body(), ToIp.class), empty());
        assertThat(expressions(predicate.body(), ToIpLeadingZerosRejected.class), not(empty()));
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        List<E> found = new ArrayList<>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }

    private static <E extends Expression> List<E> expressions(Expression expression, Class<E> type) {
        List<E> found = new ArrayList<>();
        expression.forEachDown(type, found::add);
        return found;
    }
}
