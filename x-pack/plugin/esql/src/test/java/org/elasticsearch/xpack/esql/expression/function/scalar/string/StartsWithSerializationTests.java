/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

public class StartsWithSerializationTests extends AbstractExpressionSerializationTests<StartsWith> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return EsqlScalarFunction.getNamedWriteables();
    }

    @Override
    protected StartsWith createTestInstance() {
        Source source = randomSource();
        Expression str = randomChild();
        Expression prefix = randomChild();
        return new StartsWith(source, str, prefix);
    }

    @Override
    protected StartsWith mutateInstance(StartsWith instance) throws IOException {
        Source source = instance.source();
        Expression str = instance.str();
        Expression prefix = instance.prefix();
        if (randomBoolean()) {
            str = randomValueOtherThan(str, AbstractExpressionSerializationTests::randomChild);
        } else {
            prefix = randomValueOtherThan(prefix, AbstractExpressionSerializationTests::randomChild);
        }
        return new StartsWith(source, str, prefix);
    }
}
