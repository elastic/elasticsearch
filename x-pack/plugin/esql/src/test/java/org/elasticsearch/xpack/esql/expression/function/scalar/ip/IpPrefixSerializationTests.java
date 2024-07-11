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

public class IpPrefixSerializationTests extends AbstractExpressionSerializationTests<IpPrefix> {
    @Override
    protected IpPrefix createTestInstance() {
        Source source = randomSource();
        Expression ipField = randomChild();
        Expression prefixLengthV4Field = randomChild();
        Expression prefixLengthV6Field = randomChild();
        return new IpPrefix(source, ipField, prefixLengthV4Field, prefixLengthV6Field);
    }

    @Override
    protected IpPrefix mutateInstance(IpPrefix instance) throws IOException {
        Source source = instance.source();
        Expression ipField = instance.ipField();
        Expression prefixLengthV4Field = instance.prefixLengthV4Field();
        Expression prefixLengthV6Field = instance.prefixLengthV6Field();
        switch (between(0, 2)) {
            case 0 -> ipField = randomValueOtherThan(ipField, AbstractExpressionSerializationTests::randomChild);
            case 1 -> prefixLengthV4Field = randomValueOtherThan(prefixLengthV4Field, AbstractExpressionSerializationTests::randomChild);
            case 2 -> prefixLengthV6Field = randomValueOtherThan(prefixLengthV6Field, AbstractExpressionSerializationTests::randomChild);
        }
        return new IpPrefix(source, ipField, prefixLengthV4Field, prefixLengthV6Field);
    }
}
