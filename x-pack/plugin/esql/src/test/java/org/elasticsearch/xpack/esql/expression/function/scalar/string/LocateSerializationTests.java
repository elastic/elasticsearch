/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class LocateSerializationTests extends AbstractExpressionSerializationTests<Locate> {
    @Override
    protected Locate createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression substr = randomChild();
        Expression start = randomChild();
        return new Locate(source, str, substr, start);
    }

    @Override
    protected Locate mutateInstance(Locate instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression substr = instance.substr();
        Expression start = instance.start();
        switch (between(0, 2)) {
            case 0 -> str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
            case 1 -> substr = randomValueOtherThan(substr, AbstractExpressionSerializationTests::randomChild);
            case 2 -> start = randomValueOtherThan(start, AbstractExpressionSerializationTests::randomChild);
        }
        return new Locate(source, str, substr, start);
    }
}
