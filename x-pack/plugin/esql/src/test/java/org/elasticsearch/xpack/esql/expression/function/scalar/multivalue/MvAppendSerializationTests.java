/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MvAppendSerializationTests extends AbstractExpressionSerializationTests<MvAppend> {

    private static final int randomFieldNumber = randomIntBetween(2, 10);

    @Override
    protected MvAppend createTestInstance() {
        Source source = randomSource();
        Expression field1 = randomChild();

        List<Expression> filedList = new ArrayList<>();
        for (int i = 0; i < randomFieldNumber; i++) {
            filedList.add(randomChild());
        }
        return new MvAppend(source, field1, filedList);
    }

    @Override
    protected MvAppend mutateInstance(MvAppend instance) throws IOException {
        Source source = randomSource();
        Expression field1 = randomChild();
        List<Expression> filedList = new ArrayList<>();
        for (int i = 0; i < randomFieldNumber; i++) {
            filedList.add(randomChild());
        }
        if (randomBoolean()) {
            field1 = randomValueOtherThan(field1, AbstractExpressionSerializationTests::randomChild);
        } else {
            for (int i = 0; i < randomFieldNumber; i++) {
                filedList.set(i, randomValueOtherThan(filedList.get(i), AbstractExpressionSerializationTests::randomChild));
            }
        }
        return new MvAppend(source, field1, filedList);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
