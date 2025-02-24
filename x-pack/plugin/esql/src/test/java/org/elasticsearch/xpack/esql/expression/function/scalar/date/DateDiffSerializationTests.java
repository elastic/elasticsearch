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

public class DateDiffSerializationTests extends AbstractExpressionSerializationTests<DateDiff> {
    @Override
    protected DateDiff createTestInstance() {
        Source source = randomSource();
        Expression unit = randomChild();
        Expression startTimestamp = randomChild();
        Expression endTimestamp = randomChild();
        return new DateDiff(source, unit, startTimestamp, endTimestamp);
    }

    @Override
    protected DateDiff mutateInstance(DateDiff instance) throws IOException {
        Source source = instance.source();
        Expression unit = instance.unit();
        Expression startTimestamp = instance.startTimestamp();
        Expression endTimestamp = instance.endTimestamp();
        switch (between(0, 2)) {
            case 0 -> unit = randomValueOtherThan(unit, AbstractExpressionSerializationTests::randomChild);
            case 1 -> startTimestamp = randomValueOtherThan(startTimestamp, AbstractExpressionSerializationTests::randomChild);
            case 2 -> endTimestamp = randomValueOtherThan(endTimestamp, AbstractExpressionSerializationTests::randomChild);
        }
        return new DateDiff(source, unit, startTimestamp, endTimestamp);
    }
}
