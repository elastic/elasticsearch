/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ToIntegerBaseSerializationTests extends AbstractExpressionSerializationTests<ToIntegerBase> {

    @Override
    protected ToIntegerBase createTestInstance() {
        Source source = randomSource();
        Expression string = randomChild();
        Expression base = randomChild();
        return new ToIntegerBase(source, string, base);
    }

    @Override
    protected ToIntegerBase mutateInstance(ToIntegerBase instance) throws IOException {
        Source source = instance.source();
        Expression string = instance.string();
        Expression base = instance.base();
        if (randomBoolean()) {
            string = randomValueOtherThan(string, AbstractExpressionSerializationTests::randomChild);
        } else {
            base = randomValueOtherThan(base, AbstractExpressionSerializationTests::randomChild);
        }
        return new ToIntegerBase(source, string, base);
    }
}
