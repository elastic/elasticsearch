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

public class ToDatePeriodSerializationTests extends AbstractExpressionSerializationTests<ToDatePeriod> {
    @Override
    protected ToDatePeriod createTestInstance() {
        return new ToDatePeriod(randomSource(), randomLiteral(KEYWORD));
    }

    @Override
    protected ToDatePeriod mutateInstance(ToDatePeriod instance) throws IOException {
        return null;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected ToDatePeriod copyInstance(ToDatePeriod instance, TransportVersion version) throws IOException {
        return new ToDatePeriod(instance.source(), instance.arguments().get(0));
    }
}
