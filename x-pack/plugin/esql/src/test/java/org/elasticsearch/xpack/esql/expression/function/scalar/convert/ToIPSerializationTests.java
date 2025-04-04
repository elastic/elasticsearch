/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.LiteralTests;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ToIPSerializationTests extends AbstractUnaryScalarSerializationTests<ToIP> {
    @Override
    protected ToIP create(Source source, Expression child) {
        return new ToIP(source, child, randomMapExpression());
    }

    MapExpression randomMapExpression() {
        if (randomBoolean()) {
            return null;
        }
        int entries = between(1, 10);
        Map<String, Literal> lits = new HashMap<>(entries);
        while (lits.size() < entries) {
            lits.put(randomAlphaOfLength(5), LiteralTests.randomLiteral());
        }
        List<Expression> expressions = new ArrayList<>(entries * 2);
        for (Map.Entry<String, Literal> e : lits.entrySet()) {
            expressions.add(new Literal(Source.EMPTY, e.getKey(), DataType.KEYWORD));
            expressions.add(e.getValue());
        }
        return new MapExpression(Source.EMPTY, expressions);
    }
}
