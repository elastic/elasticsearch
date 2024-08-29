/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.optimizer;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.TestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.of;
import static org.elasticsearch.xpack.esql.core.util.TestUtils.rangeOf;

public class OptimizerRulesTests extends ESTestCase {

    private static final Literal FIVE = L(5);
    private static final Literal SIX = L(6);

    public static class DummyBooleanExpression extends Expression {

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

    private static Literal L(Object value) {
        return of(value);
    }

    private static FieldAttribute getFieldAttribute() {
        return TestUtils.getFieldAttribute("a");
    }

    //
    // Range optimization
    //

    // 6 < a <= 5 -> FALSE
    public void testFoldExcludingRangeToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = rangeOf(fa, SIX, false, FIVE, true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = rangeOf(fa, SIX, false, L(5.5d), true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // Conjunction

}
