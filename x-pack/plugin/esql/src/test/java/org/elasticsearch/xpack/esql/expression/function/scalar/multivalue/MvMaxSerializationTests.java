/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvMaxSerializationTests extends AbstractExpressionSerializationTests<MvMax> {
    @Override
    protected MvMax createTestInstance() {
        return new MvMax(randomSource(), randomChild());
    }

    @Override
    protected MvMax mutateInstance(MvMax instance) throws IOException {
        return new MvMax(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
