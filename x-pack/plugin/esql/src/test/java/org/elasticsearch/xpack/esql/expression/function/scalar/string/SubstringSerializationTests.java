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

public class SubstringSerializationTests extends AbstractExpressionSerializationTests<Substring> {
    @Override
    protected Substring createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression start = randomChild();
        Expression length = randomChild();
        return new Substring(source, str, start, length);
    }

    @Override
    protected Substring mutateInstance(Substring instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression start = instance.start();
        Expression length = instance.length();
        switch (between(0, 2)) {
            case 0 -> str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
            case 1 -> start = randomValueOtherThan(start, AbstractExpressionSerializationTests::randomChild);
            case 2 -> length = randomValueOtherThan(length, AbstractExpressionSerializationTests::randomChild);
        }
        return new Substring(source, str, start, length);
    }
}
