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

public class DateParseSerializationTests extends AbstractExpressionSerializationTests<DateParse> {
    @Override
    protected DateParse createTestInstance() {
        Source source = randomSource();
        Expression first = randomChild();
        Expression second = randomBoolean() ? null : randomChild();
        return new DateParse(source, first, second);
    }

    @Override
    protected DateParse mutateInstance(DateParse instance) throws IOException {
        Source source = instance.source();
        Expression first = instance.children().get(0);
        Expression second = instance.children().size() == 1 ? null : instance.children().get(1);
        if (randomBoolean()) {
            first = randomValueOtherThan(first, AbstractExpressionSerializationTests::randomChild);
        } else {
            second = randomValueOtherThan(second, () -> randomBoolean() ? null : randomChild());
        }
        return new DateParse(source, first, second);
    }
}
