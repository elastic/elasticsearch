/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class DateFormatSerializationTests extends AbstractExpressionSerializationTests<DateFormat> {
    @Override
    protected DateFormat createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression format = randomBoolean() ? null : randomChild();
        return new DateFormat(source, field, format, configuration());
    }

    @Override
    protected DateFormat mutateInstance(DateFormat instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression format = instance.format();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            format = randomValueOtherThan(format, () -> randomBoolean() ? null : randomChild());
        }
        return new DateFormat(source, field, format, configuration());
    }
}
