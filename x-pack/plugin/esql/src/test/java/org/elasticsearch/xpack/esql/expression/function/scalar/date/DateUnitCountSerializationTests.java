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

public class DateUnitCountSerializationTests extends AbstractExpressionSerializationTests<DateUnitCount> {
    @Override
    protected DateUnitCount createTestInstance() {
        Source source = randomSource();
        Expression toUnit = randomChild();
        Expression fromUnit = randomChild();
        Expression date = randomChild();
        return new DateUnitCount(source, toUnit, fromUnit, date, configuration());
    }

    @Override
    protected DateUnitCount mutateInstance(DateUnitCount instance) throws IOException {
        Source source = instance.source();
        Expression toUnit = instance.children().get(0);
        Expression fromUnit = instance.children().get(1);
        Expression date = instance.children().get(2);
        switch (between(0, 2)) {
            case 0 -> toUnit = randomValueOtherThan(toUnit, AbstractExpressionSerializationTests::randomChild);
            case 1 -> fromUnit = randomValueOtherThan(fromUnit, AbstractExpressionSerializationTests::randomChild);
            case 2 -> date = randomValueOtherThan(date, AbstractExpressionSerializationTests::randomChild);
        }
        return new DateUnitCount(source, toUnit, fromUnit, date, configuration());
    }
}
