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

public class DateExtractSerializationTests extends AbstractExpressionSerializationTests<DateExtract> {
    @Override
    protected DateExtract createTestInstance() {
        Source source = randomSource();
        Expression datePart = randomChild();
        Expression field = randomChild();
        return new DateExtract(source, datePart, field, configuration());
    }

    @Override
    protected DateExtract mutateInstance(DateExtract instance) throws IOException {
        Source source = instance.source();
        Expression datePart = instance.datePart();
        Expression field = instance.field();
        if (randomBoolean()) {
            datePart = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        }
        return new DateExtract(source, datePart, field, configuration());
    }
}
