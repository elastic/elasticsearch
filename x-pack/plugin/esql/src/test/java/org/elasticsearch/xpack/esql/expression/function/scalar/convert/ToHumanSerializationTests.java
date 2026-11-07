/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class ToHumanSerializationTests extends AbstractExpressionSerializationTests<ToHuman> {
    @Override
    protected ToHuman createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression unit = new Literal(
            Source.EMPTY,
            new BytesRef(randomFrom("duration", "bits", "bytes", "percent")),
            DataType.KEYWORD
        );
        Expression targetUnit = randomBoolean()
            ? null
            : new Literal(Source.EMPTY, new BytesRef("GB"), DataType.KEYWORD);
        return new ToHuman(source, field, unit, targetUnit);
    }

    @Override
    protected ToHuman mutateInstance(ToHuman instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression unit = instance.unit();
        Expression targetUnit = instance.targetUnit();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> unit = randomValueOtherThan(unit, AbstractExpressionSerializationTests::randomChild);
            case 2 -> {
                if (targetUnit == null) {
                    targetUnit = new Literal(
                        Source.EMPTY,
                        new BytesRef("GB"),
                        DataType.KEYWORD
                    );
                } else {
                    targetUnit = null;
                }
            }
        }
        return new ToHuman(source, field, unit, targetUnit);
    }
}
