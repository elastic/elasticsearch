/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class Sha1SerializationTests extends AbstractExpressionSerializationTests<Sha1> {

    @Override
    protected Sha1 createTestInstance() {
        return new Sha1(randomSource(), randomChild());
    }

    @Override
    protected Sha1 mutateInstance(Sha1 instance) throws IOException {
        return new Sha1(instance.source(), mutateExpression(instance.field()));
    }
}
