/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.expression.Nullability.FALSE;
import static org.elasticsearch.xpack.ql.expression.Nullability.TRUE;
import static org.elasticsearch.xpack.ql.expression.Nullability.UNKNOWN;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class NullabilityTests extends ESTestCase {

    public static class Nullable extends LeafExpression {

        private final Nullability nullability;

        public Nullable(Source source, Nullability nullability) {
            super(source);
            this.nullability = nullability;
        }

        @Override
        public Nullability nullable() {
            return nullability;
        }

        @Override
        public DataType dataType() {
            return DataTypes.BOOLEAN;
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, Nullable::new, nullability);
        }
    }

    private Nullable YES = new Nullable(EMPTY, TRUE);
    private Nullable NO = new Nullable(EMPTY, FALSE);
    private Nullable MAYBE = new Nullable(EMPTY, UNKNOWN);

    public void testLogicalAndOfNullabilities() {
        assertEquals(TRUE, Expressions.nullable(asList(YES)));
        assertEquals(FALSE, Expressions.nullable(asList(NO)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE)));

        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, MAYBE)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, YES)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, NO)));

        assertEquals(FALSE, Expressions.nullable(asList(NO, NO)));
        assertEquals(TRUE, Expressions.nullable(asList(NO, YES)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(NO, MAYBE)));

        assertEquals(TRUE, Expressions.nullable(asList(YES, YES)));
        assertEquals(TRUE, Expressions.nullable(asList(YES, NO)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(YES, MAYBE)));
    }
}
