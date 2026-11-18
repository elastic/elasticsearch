/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexSortConfig.SortEntry;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The {@code index.sort} expression is parsed by a hand written parser in {@code server}, which must accept a strict subset of the
 * ES|QL {@code SORT} syntax so that a stored expression keeps its meaning if it is later parsed by ES|QL. This runs the same strings
 * through both parsers and checks that they agree.
 */
public class IndexSortExpressionParityTests extends AbstractStatementParserTests {

    private static final List<String> ACCEPTED = List.of(
        "a",
        "a DESC",
        "a.b.c asc",
        "@timestamp DESC",
        "_x, @y_1",
        "`pod-name` DESC",
        "`a``b`, `c d` DESC NULLS LAST",
        "a NULLS FIRST, b DESC NULLS LAST, c ASC NULLS LAST, d DESC NULLS FIRST",
        "a\tDESC,\n b  nulls   first ,  c",
        "MV_MIN(a), MV_MAX(a) DESC",
        "mv_max( a.b )  desc nulls last, Mv_Min(`x y`)",
        "MV_MAX(a), a",
        "`MV_MAX(a)`, MV_MAX(a)",
        "mv_max, `mv_min`"
    );

    /** Strings that ES|QL doesn't read as a plain field, so the server parser must not either. */
    private static final List<String> NOT_PLAIN_FIELDS_IN_ESQL = List.of(
        "pod-name",
        "pod-name DESC",
        "a+b",
        "1x",
        "a..b",
        "a.",
        "foo*",
        "null",
        "asc",
        "true DESC",
        "foo.desc",
        "null.foo",
        "where",
        "a.nulls DESC",
        "ABS(a)"
    );

    /** Strings that are rejected by the server parser for its own restrictions, even if ES|QL accepts them. */
    private static final List<String> REJECTED_BY_SERVER_ONLY = List.of("a, a DESC", "MV_MAX(a), mv_max(a)", "a,", "a SIDEWAYS");

    public void testAcceptedExpressionsMeanTheSameInEsql() {
        for (String expression : ACCEPTED) {
            List<SortEntry> expected = IndexSortConfig.parseSortExpression(expression);
            List<Order> actual = esqlOrders(expression);
            assertThat(expression, actual, hasSize(expected.size()));
            for (int i = 0; i < expected.size(); i++) {
                SortEntry entry = expected.get(i);
                Order order = actual.get(i);
                String message = expression + " entry " + i;
                assertThat(message, order.direction() == Order.OrderDirection.DESC, equalTo(entry.order() == SortOrder.DESC));
                assertThat(message, order.nullsPosition() == Order.NullsPosition.FIRST ? "_first" : "_last", equalTo(entry.missing()));
                Expression child = order.child();
                if (child instanceof UnresolvedFunction function) {
                    assertThat(
                        message,
                        function.name().toUpperCase(Locale.ROOT),
                        equalTo(entry.mode().name().equals("MAX") ? "MV_MAX" : "MV_MIN")
                    );
                    assertThat(message, function.children(), hasSize(1));
                    child = function.children().get(0);
                } else {
                    assertThat(message, entry.mode().name(), equalTo(entry.order() == SortOrder.DESC ? "MAX" : "MIN"));
                }
                assertThat(message, child, instanceOf(UnresolvedAttribute.class));
                assertThat(message, ((UnresolvedAttribute) child).name(), equalTo(entry.field()));
            }
        }
    }

    public void testExpressionsThatEsqlDoesNotReadAsPlainFieldsAreRejected() {
        for (String expression : NOT_PLAIN_FIELDS_IN_ESQL) {
            expectThrows(IllegalArgumentException.class, expression, () -> IndexSortConfig.parseSortExpression(expression));
            List<Order> orders;
            try {
                orders = esqlOrders(expression);
            } catch (Exception e) {
                continue; // ES|QL rejects it too
            }
            Order first = orders.get(0);
            Expression child = first.child();
            if (child instanceof UnresolvedFunction function && function.children().size() == 1) {
                child = function.children().get(0);
            }
            // If ES|QL accepts it, it must not read it as the single field name the server parser would have used.
            String firstToken = expression.trim().split("[\\s,]+")[0];
            assertFalse(
                "ES|QL reads [" + expression + "] as the plain field [" + firstToken + "]",
                child instanceof UnresolvedAttribute attribute && attribute.name().equals(firstToken)
            );
        }
    }

    public void testServerOnlyRestrictions() {
        for (String expression : REJECTED_BY_SERVER_ONLY) {
            expectThrows(IllegalArgumentException.class, expression, () -> IndexSortConfig.parseSortExpression(expression));
        }
    }

    private List<Order> esqlOrders(String expression) {
        LogicalPlan plan = query("FROM i | SORT " + expression);
        assertThat(plan, instanceOf(OrderBy.class));
        return ((OrderBy) plan).order();
    }
}
