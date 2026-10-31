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

public class FieldExtractFlattenedSerializationTests extends AbstractExpressionSerializationTests<FieldExtractFlattened> {
    @Override
    protected FieldExtractFlattened createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression path = randomChild();
        Expression injectedKey = randomBoolean() ? randomChild() : null;
        return new FieldExtractFlattened(source, field, path, injectedKey);
    }

    @Override
    protected FieldExtractFlattened mutateInstance(FieldExtractFlattened instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.children().get(0);
        Expression path = instance.children().get(1);
        Expression injectedKey = instance.children().size() > 2 ? instance.children().get(2) : null;
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> path = randomValueOtherThan(path, AbstractExpressionSerializationTests::randomChild);
            case 2 -> {
                if (injectedKey == null) {
                    injectedKey = randomChild();
                } else {
                    injectedKey = randomBoolean()
                        ? null
                        : randomValueOtherThan(injectedKey, AbstractExpressionSerializationTests::randomChild);
                }
            }
        }
        return new FieldExtractFlattened(source, field, path, injectedKey);
    }
}
