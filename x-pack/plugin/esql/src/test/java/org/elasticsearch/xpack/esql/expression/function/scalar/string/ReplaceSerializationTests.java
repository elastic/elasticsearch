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

public class ReplaceSerializationTests extends AbstractExpressionSerializationTests<Replace> {
    @Override
    protected Replace createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression regex = randomChild();
        Expression newStr = randomChild();
        return new Replace(source, str, regex, newStr);
    }

    @Override
    protected Replace mutateInstance(Replace instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression regex = instance.regex();
        Expression newStr = instance.newStr();
        switch (between(0, 2)) {
            case 0 -> str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
            case 1 -> regex = randomValueOtherThan(regex, AbstractExpressionSerializationTests::randomChild);
            case 2 -> newStr = randomValueOtherThan(newStr, AbstractExpressionSerializationTests::randomChild);
        }
        return new Replace(source, str, regex, newStr);
    }
}
