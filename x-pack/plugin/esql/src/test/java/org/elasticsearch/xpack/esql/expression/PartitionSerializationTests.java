/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class PartitionSerializationTests extends AbstractExpressionSerializationTests<Partition> {
    public static Partition randomPartition() {
        return new Partition(randomSource(), randomChild());
    }

    @Override
    protected Partition createTestInstance() {
        return randomPartition();
    }

    @Override
    protected Partition mutateInstance(Partition instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.child();
        child = randomValueOtherThan(child, AbstractExpressionSerializationTests::randomChild);
        return new Partition(source, child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
