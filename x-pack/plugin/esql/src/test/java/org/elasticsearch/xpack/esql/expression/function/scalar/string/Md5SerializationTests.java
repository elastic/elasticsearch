/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class Md5SerializationTests extends AbstractExpressionSerializationTests<Md5> {

    @Override
    protected Md5 createTestInstance() {
        return new Md5(randomSource(), randomChild());
    }

    @Override
    protected Md5 mutateInstance(Md5 instance) throws IOException {
        return new Md5(instance.source(), mutateExpression(instance.field()));
    }
}
