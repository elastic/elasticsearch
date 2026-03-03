/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class MultiMatchSerializationTests extends AbstractExpressionSerializationTests<MultiMatch> {
    @Override
    protected MultiMatch createTestInstance() {
        Source source = randomSource();
        Expression query = randomChild();
        List<Expression> fields = randomList(1, 5, AbstractExpressionSerializationTests::randomChild);
        return new MultiMatch(source, query, fields, null);
    }

    @Override
    protected MultiMatch mutateInstance(MultiMatch instance) throws IOException {
        Source source = instance.source();
        Expression query = instance.query();
        List<Expression> fields = instance.fields();
        if (randomBoolean()) {
            query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
        } else {
            fields = randomValueOtherThan(fields, () -> randomList(1, 5, AbstractExpressionSerializationTests::randomChild));
        }
        return new MultiMatch(source, query, fields, null);
    }
}
