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

public class DaysInMonthSerializationTests extends AbstractExpressionSerializationTests<DaysInMonth> {
    @Override
    protected DaysInMonth createTestInstance() {
        Source source = randomSource();
        Expression date = randomChild();
        return new DaysInMonth(source, date, configuration());
    }

    @Override
    protected DaysInMonth mutateInstance(DaysInMonth instance) throws IOException {
        Source source = instance.source();
        Expression date = instance.field();
        return new DaysInMonth(source, randomValueOtherThan(date, AbstractExpressionSerializationTests::randomChild), configuration());
    }
}
