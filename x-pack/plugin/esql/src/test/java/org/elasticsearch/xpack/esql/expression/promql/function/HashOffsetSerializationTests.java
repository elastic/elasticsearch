/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HashOffsetSerializationTests extends AbstractExpressionSerializationTests<HashOffset> {
    @Override
    protected HashOffset createTestInstance() {
        return new HashOffset(randomSource(), randomList(0, 3, AbstractExpressionSerializationTests::randomChild));
    }

    @Override
    protected HashOffset mutateInstance(HashOffset instance) throws IOException {
        List<Expression> keys = new ArrayList<>(instance.children());
        if (keys.isEmpty() || randomBoolean()) {
            keys.add(randomChild());
        } else if (randomBoolean()) {
            keys.remove(randomInt(keys.size() - 1));
        } else {
            int idx = randomInt(keys.size() - 1);
            keys.set(idx, randomValueOtherThan(keys.get(idx), AbstractExpressionSerializationTests::randomChild));
        }
        return new HashOffset(instance.source(), keys);
    }
}
