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

public class UriPartsSerializationTests extends AbstractExpressionSerializationTests<UriParts> {
    @Override
    protected UriParts createTestInstance() {
        return new UriParts(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected UriParts mutateInstance(UriParts instance) throws IOException {
        Source source = instance.source();
        Expression first = instance.children().get(0);
        Expression second = instance.children().get(1);

        return new UriParts(
            source,
            randomValueOtherThan(first, AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(second, AbstractExpressionSerializationTests::randomChild)
        );
    }
}
