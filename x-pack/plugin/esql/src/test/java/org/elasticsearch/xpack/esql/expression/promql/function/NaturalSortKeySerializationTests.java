/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class NaturalSortKeySerializationTests extends AbstractExpressionSerializationTests<NaturalSortKey> {
    @Override
    protected NaturalSortKey createTestInstance() {
        return new NaturalSortKey(randomSource(), randomChild());
    }

    @Override
    protected NaturalSortKey mutateInstance(NaturalSortKey instance) throws IOException {
        Source source = instance.source();
        Expression field = randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild);
        return new NaturalSortKey(source, field);
    }
}
