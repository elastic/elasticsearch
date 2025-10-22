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
        Expression third = randomBoolean() ? randomChild() : null;
        return new DateParse(source, first, second, third);
    }

    @Override
    protected DateParse mutateInstance(DateParse instance) throws IOException {
        Source source = instance.source();
        Expression first = instance.children().get(0);
        Expression second = instance.children().size() > 1 ? instance.children().get(1) : null;
        Expression third = instance.children().size() > 2 ? instance.children().get(2) : null;
        switch (between(0, 2)) {
            case 0 -> first = randomValueOtherThan(first, AbstractExpressionSerializationTests::randomChild);
            case 1 -> second = randomValueOtherThan(second, () -> randomBoolean() ? null : randomChild());
            case 2 -> third = randomValueOtherThan(third, () -> randomBoolean() ? null : randomChild());
        }
        return new DateParse(source, first, second, third);
    }
}
