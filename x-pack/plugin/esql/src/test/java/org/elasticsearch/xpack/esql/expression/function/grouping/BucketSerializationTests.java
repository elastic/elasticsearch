/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class BucketSerializationTests extends AbstractExpressionSerializationTests<Bucket> {
    @Override
    protected Bucket createTestInstance() {
        return createRandomBucket();
    }

    public static Bucket createRandomBucket() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression buckets = randomChild();
        Expression from = randomChild();
        Expression to = randomChild();
        return new Bucket(source, field, buckets, from, to);
    }

    @Override
    protected Bucket mutateInstance(Bucket instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression buckets = instance.buckets();
        Expression from = instance.from();
        Expression to = instance.to();
        switch (between(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> buckets = randomValueOtherThan(buckets, AbstractExpressionSerializationTests::randomChild);
            case 2 -> from = randomValueOtherThan(from, AbstractExpressionSerializationTests::randomChild);
            case 3 -> to = randomValueOtherThan(to, AbstractExpressionSerializationTests::randomChild);
        }
        return new Bucket(source, field, buckets, from, to);
    }
}
