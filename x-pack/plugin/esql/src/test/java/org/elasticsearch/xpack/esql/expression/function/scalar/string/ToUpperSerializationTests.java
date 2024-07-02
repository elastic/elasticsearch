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

public class ToUpperSerializationTests extends AbstractExpressionSerializationTests<ToUpper> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return EsqlScalarFunction.getNamedWriteables();
    }

    @Override
    protected ToUpper createTestInstance() {
        return new ToUpper(randomSource(), randomChild(), configuration());
    }

    @Override
    protected ToUpper mutateInstance(ToUpper instance) throws IOException {
        Source source = instance.source();
        Expression child = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new ToUpper(source, child, configuration());
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
