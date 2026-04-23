/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

public class SparklineSerializationTests extends AbstractExpressionSerializationTests<Sparkline> {
    @Override
    protected Sparkline createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression key = randomChild();
        Expression buckets = randomChild();
        Expression from = randomChild();
        Expression to = randomChild();

        return new Sparkline(source, field, key, buckets, from, to);
    }

    @Override
    protected Sparkline mutateInstance(Sparkline instance) {
        Source source = instance.source();
        Expression field = instance.field();
        Expression key = instance.key();
        Expression buckets = instance.buckets();
        Expression from = instance.from();
        Expression to = instance.to();

        switch (between(0, 4)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> key = randomValueOtherThan(key, AbstractExpressionSerializationTests::randomChild);
            case 2 -> buckets = randomValueOtherThan(buckets, AbstractExpressionSerializationTests::randomChild);
            case 3 -> from = randomValueOtherThan(from, AbstractExpressionSerializationTests::randomChild);
            case 4 -> to = randomValueOtherThan(to, AbstractExpressionSerializationTests::randomChild);
        }
        return new Sparkline(source, field, key, buckets, from, to);
    }
}
