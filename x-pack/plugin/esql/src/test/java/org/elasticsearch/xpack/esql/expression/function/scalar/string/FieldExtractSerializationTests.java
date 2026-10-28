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

public class FieldExtractSerializationTests extends AbstractExpressionSerializationTests<FieldExtract> {
    @Override
    protected FieldExtract createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression path = randomChild();
        return new FieldExtract(source, field, path);
    }

    @Override
    protected FieldExtract mutateInstance(FieldExtract instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression path = instance.path();
        switch (between(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> path = randomValueOtherThan(path, AbstractExpressionSerializationTests::randomChild);
        }
        return new FieldExtract(source, field, path);
    }
}
