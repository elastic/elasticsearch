/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class ToTimeDurationSerializationTests extends AbstractExpressionSerializationTests<ToTimeDuration> {
    @Override
    protected ToTimeDuration createTestInstance() {
        return new ToTimeDuration(randomSource(), randomLiteral(KEYWORD));
    }

    @Override
    protected ToTimeDuration mutateInstance(ToTimeDuration instance) throws IOException {
        return null;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected ToTimeDuration copyInstance(ToTimeDuration instance, TransportVersion version) throws IOException {
        return new ToTimeDuration(instance.source(), instance.arguments().get(0));
    }
}
