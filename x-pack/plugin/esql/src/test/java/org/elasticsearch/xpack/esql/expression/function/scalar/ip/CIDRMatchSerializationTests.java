/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class CIDRMatchSerializationTests extends AbstractExpressionSerializationTests<CIDRMatch> {
    @Override
    protected CIDRMatch createTestInstance() {
        Source source = randomSource();
        Expression ipField = randomChild();
        List<Expression> matches = randomList(1, 10, AbstractExpressionSerializationTests::randomChild);
        return new CIDRMatch(source, ipField, matches);
    }

    @Override
    protected CIDRMatch mutateInstance(CIDRMatch instance) throws IOException {
        Source source = instance.source();
        Expression ipField = instance.ipField();
        List<Expression> matches = instance.matches();
        if (randomBoolean()) {
            ipField = randomValueOtherThan(ipField, AbstractExpressionSerializationTests::randomChild);
        } else {
            matches = randomValueOtherThan(matches, () -> randomList(1, 10, AbstractExpressionSerializationTests::randomChild));
        }
        return new CIDRMatch(source, ipField, matches);
    }
}
