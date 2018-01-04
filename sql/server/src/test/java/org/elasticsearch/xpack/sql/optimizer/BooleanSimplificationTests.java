/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanSimplification;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.Collections;

public class BooleanSimplificationTests extends ESTestCase {

    private static final Expression DUMMY_EXPRESSION = new DummyBooleanExpression(Location.EMPTY, 0);

    private static class DummyBooleanExpression extends Expression {
        
        private final int id;
        
        public DummyBooleanExpression(Location location, int id) {
            super(location, Collections.emptyList());
            this.id = id;
        }
        
        @Override
        public boolean nullable() {
            return false;
        }

        @Override
        public DataType dataType() {
            return DataTypes.BOOLEAN;
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

    public void testSimplifyOr() {
        BooleanSimplification simplification = new BooleanSimplification();
        
        assertEquals(Literal.TRUE, simplification.rule(new Or(Location.EMPTY, Literal.TRUE, Literal.TRUE)));
        assertEquals(Literal.TRUE, simplification.rule(new Or(Location.EMPTY, Literal.TRUE, DUMMY_EXPRESSION)));
        assertEquals(Literal.TRUE, simplification.rule(new Or(Location.EMPTY, DUMMY_EXPRESSION, Literal.TRUE)));

        assertEquals(Literal.FALSE, simplification.rule(new Or(Location.EMPTY, Literal.FALSE, Literal.FALSE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(Location.EMPTY, Literal.FALSE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(Location.EMPTY, DUMMY_EXPRESSION, Literal.FALSE)));
    }

    public void testSimplifyAnd() {
        BooleanSimplification simplification = new BooleanSimplification();
        
        assertEquals(Literal.TRUE, simplification.rule(new And(Location.EMPTY, Literal.TRUE, Literal.TRUE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(Location.EMPTY, Literal.TRUE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(Location.EMPTY, DUMMY_EXPRESSION, Literal.TRUE)));

        assertEquals(Literal.FALSE, simplification.rule(new And(Location.EMPTY, Literal.FALSE, Literal.FALSE)));
        assertEquals(Literal.FALSE, simplification.rule(new And(Location.EMPTY, Literal.FALSE, DUMMY_EXPRESSION)));
        assertEquals(Literal.FALSE, simplification.rule(new And(Location.EMPTY, DUMMY_EXPRESSION, Literal.FALSE)));
    }

    public void testCommonFactorExtraction() {
        BooleanSimplification simplification = new BooleanSimplification();

        Expression a = new DummyBooleanExpression(Location.EMPTY, 1);
        Expression b = new DummyBooleanExpression(Location.EMPTY, 2);
        Expression c = new DummyBooleanExpression(Location.EMPTY, 3);

        Expression actual = new Or(Location.EMPTY, new And(Location.EMPTY, a, b), new And(Location.EMPTY, a, c));
        Expression expected = new And(Location.EMPTY, a, new Or(Location.EMPTY, b, c));

        assertEquals(expected, simplification.rule(actual));
    }
}
