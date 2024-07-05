/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.LiteralTests;

import java.io.IOException;

public class LiteralSerializationTests extends AbstractExpressionSerializationTests<Literal> {
    @Override
    protected Literal createTestInstance() {
        return LiteralTests.randomLiteral();
    }

    @Override
    protected Literal mutateInstance(Literal instance) throws IOException {
        return LiteralTests.mutateLiteral(instance);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
