/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.parser.EsqlParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.rangeOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.of;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class OptimizerRulesTests extends ESTestCase {

    private static final Literal FIVE = of(5);
    private static final Literal SIX = of(6);

    public static final class DummyBooleanExpression extends Expression {

        private final int id;

        public DummyBooleanExpression(Source source, int id) {
            super(source, Collections.emptyList());
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, DummyBooleanExpression::new, id);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children");
        }

        @Override
        public Nullability nullable() {
            return Nullability.FALSE;
        }

        @Override
        public DataType dataType() {
            return BOOLEAN;
        }

        @Override
        public int hashCode() {
            int h = getClass().hashCode();
            h = 31 * h + id;
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return id == ((DummyBooleanExpression) obj).id;
        }
    }

    //
    // Range optimization
    //

    // 6 < a <= 5 -> FALSE
    public void testFoldExcludingRangeToFalse() {
        FieldAttribute fa = getFieldAttribute("a");

        Range r = rangeOf(fa, SIX, false, FIVE, true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold(FoldContext.small()));
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = getFieldAttribute("a");

        Range r = rangeOf(fa, SIX, false, of(5.5d), true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold(FoldContext.small()));
    }

    public void testOptimizerExpressionRuleShouldNotVisitExcludedNodes() {
        var rule = new OptimizerRules.OptimizerExpressionRule<>(randomFrom(OptimizerRules.TransformDirection.values())) {
            private final List<Expression> appliedTo = new ArrayList<>();

            @Override
            protected Expression rule(Expression e, LogicalOptimizerContext ctx) {
                appliedTo.add(e);
                return e;
            }
        };

        rule.apply(
            new EsqlParser().createStatement("FROM index | EVAL x=f1+1 | KEEP x, f2 | LIMIT 1"),
            new LogicalOptimizerContext(null, FoldContext.small())
        );

        var literal = new Literal(new Source(1, 25, "1"), 1, DataType.INTEGER);
        var attribute = new UnresolvedAttribute(new Source(1, 20, "f1"), "f1");
        var add = new Add(new Source(1, 20, "f1+1"), attribute, literal);
        var alias = new Alias(new Source(1, 18, "x=f1+1"), "x", add);

        // contains expressions only from EVAL
        assertThat(rule.appliedTo, containsInAnyOrder(alias, add, attribute, literal));
    }
}
