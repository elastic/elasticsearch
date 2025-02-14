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

public class MvZipSerializationTests extends AbstractExpressionSerializationTests<MvZip> {
    @Override
    protected MvZip createTestInstance() {
        Source source = randomSource();
        Expression mvLeft = randomChild();
        Expression mvRight = randomChild();
        Expression delim = randomBoolean() ? null : randomChild();
        return new MvZip(source, mvLeft, mvRight, delim);
    }

    @Override
    protected MvZip mutateInstance(MvZip instance) throws IOException {
        Source source = instance.source();
        Expression mvLeft = instance.mvLeft();
        Expression mvRight = instance.mvRight();
        Expression delim = instance.delim();
        switch (between(0, 2)) {
            case 0 -> mvLeft = randomValueOtherThan(mvLeft, AbstractExpressionSerializationTests::randomChild);
            case 1 -> mvRight = randomValueOtherThan(mvRight, AbstractExpressionSerializationTests::randomChild);
            case 2 -> delim = randomValueOtherThan(delim, () -> randomBoolean() ? null : randomChild());
        }
        return new MvZip(source, mvLeft, mvRight, delim);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
