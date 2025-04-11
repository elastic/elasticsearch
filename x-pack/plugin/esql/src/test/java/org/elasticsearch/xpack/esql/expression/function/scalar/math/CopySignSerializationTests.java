/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
* or more contributor license agreements. Licensed under the Elastic License
* 2.0; you may not use this file except in compliance with the Elastic License
* 2.0.
*/

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class CopySignSerializationTests extends AbstractExpressionSerializationTests<CopySign> {
    @Override
    protected CopySign createTestInstance() {
        Source source = randomSource();
        Expression magnitude = randomChild();
        Expression sign = randomChild();
        return new CopySign(source, magnitude, sign);
    }

    @Override
    protected CopySign mutateInstance(CopySign instance) throws IOException {
        Source source = instance.source();
        Expression magnitude = instance.magnitude();
        Expression sign = instance.sign();
        if (randomBoolean()) {
            magnitude = randomValueOtherThan(magnitude, AbstractExpressionSerializationTests::randomChild);
        } else {
            sign = randomValueOtherThan(sign, AbstractExpressionSerializationTests::randomChild);
        }
        return new CopySign(source, magnitude, sign);
    }
}
