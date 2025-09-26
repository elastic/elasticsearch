/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MvMinSerializationTests extends AbstractExpressionSerializationTests<MvMin> {
    @Override
    protected MvMin createTestInstance() {
        return new MvMin(randomSource(), randomChild());
    }

    @Override
    protected MvMin mutateInstance(MvMin instance) throws IOException {
        return new MvMin(instance.source(), randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild));
    }
}
