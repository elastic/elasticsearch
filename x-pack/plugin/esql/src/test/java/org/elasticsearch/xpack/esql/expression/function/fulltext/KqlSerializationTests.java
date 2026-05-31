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

public class KqlSerializationTests extends AbstractExpressionSerializationTests<Kql> {
    @Override
    protected Kql createTestInstance() {
        Source source = randomSource();
        Expression query = randomChild();
        return new Kql(source, query, null, configuration());
    }

    @Override
    protected Kql mutateInstance(Kql instance) throws IOException {
        Source source = instance.source();
        Expression query = randomValueOtherThan(instance.query(), AbstractExpressionSerializationTests::randomChild);
        return new Kql(source, query, null, configuration());
    }
}
