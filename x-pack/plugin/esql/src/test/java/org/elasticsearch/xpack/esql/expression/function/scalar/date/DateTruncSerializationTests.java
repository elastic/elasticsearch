/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

public class DateTruncSerializationTests extends AbstractExpressionSerializationTests<DateTrunc> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return EsqlScalarFunction.getNamedWriteables();
    }

    @Override
    protected DateTrunc createTestInstance() {
        Source source = randomSource();
        Expression interval = randomChild();
        Expression field = randomChild();
        return new DateTrunc(source, interval, field);
    }

    @Override
    protected DateTrunc mutateInstance(DateTrunc instance) throws IOException {
        Source source = instance.source();
        Expression interval = instance.interval();
        Expression field = instance.field();
        if (randomBoolean()) {
            interval = randomValueOtherThan(interval, AbstractExpressionSerializationTests::randomChild);
        } else {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        }
        return new DateTrunc(source, interval, field);
    }
}
