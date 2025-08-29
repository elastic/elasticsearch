/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;

public class RoundToSerializationTests extends AbstractExpressionSerializationTests<RoundTo> {
    @Override
    protected RoundTo createTestInstance() {
        Source source = randomSource();
        DataType type = randomFrom(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.DATETIME, DataType.DATE_NANOS);
        Expression field = randomField(type);
        List<Expression> points = randomPoints(type);
        return new RoundTo(source, field, points);
    }

    private Expression randomField(DataType type) {
        return new ReferenceAttribute(Source.EMPTY, randomAlphanumericOfLength(4), randomLiteral(type).dataType());
    }

    private List<Expression> randomPoints(DataType type) {
        int length = between(1, 100);
        List<Expression> points = new ArrayList<>(length);
        while (points.size() < length) {
            points.add(randomLiteral(type));
        }
        ;
        return points;
    }

    @Override
    protected RoundTo mutateInstance(RoundTo instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        List<Expression> points = instance.points();
        DataType type = field.dataType();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, () -> randomField(type));
        } else {
            points = randomValueOtherThan(points, () -> randomPoints(type));
        }
        return new RoundTo(source, field, points);
    }
}
