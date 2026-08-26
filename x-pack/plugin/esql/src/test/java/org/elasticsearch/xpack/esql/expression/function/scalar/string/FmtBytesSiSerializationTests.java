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

public class FmtBytesSiSerializationTests extends AbstractExpressionSerializationTests<FmtBytesSi> {
    @Override
    protected FmtBytesSi createTestInstance() {
        Source source = randomSource();
        Expression bytes = randomChild();
        return new FmtBytesSi(source, bytes);
    }

    @Override
    protected FmtBytesSi mutateInstance(FmtBytesSi instance) throws IOException {
        Source source = instance.source();
        Expression bytes = instance.field();
        bytes = randomValueOtherThan(bytes, AbstractExpressionSerializationTests::randomChild);
        return new FmtBytesSi(source, bytes);
    }
}
