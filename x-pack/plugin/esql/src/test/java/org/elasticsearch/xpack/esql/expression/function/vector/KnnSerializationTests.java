/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class KnnSerializationTests extends AbstractExpressionSerializationTests<Knn> {
    @Override
    protected Knn createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        List<Expression> filterExpressions = randomList(0, 3, AbstractExpressionSerializationTests::randomChild);
        return new Knn(source, field, query, null, null, null, filterExpressions);
    }

    @Override
    protected Knn mutateInstance(Knn instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        List<Expression> filterExpressions = instance.filterExpressions();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 2 -> filterExpressions = randomValueOtherThan(
                filterExpressions,
                () -> randomList(0, 3, AbstractExpressionSerializationTests::randomChild)
            );
        }
        return new Knn(source, field, query, null, null, null, filterExpressions);
    }
}
