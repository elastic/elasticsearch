/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CountSerializationTests extends AbstractExpressionSerializationTests<Count> {
    @Override
    protected Count createTestInstance() {
        return new Count(randomSource(), randomChild(), randomOptionalChild());
    }

    @Override
    protected Count mutateInstance(Count instance) throws IOException {
        Expression field = instance.field();
        Expression bucket = instance.bucket();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            bucket = randomValueOtherThan(bucket, CountSerializationTests::randomOptionalChild);
        }
        return new Count(instance.source(), field, bucket);
    }

    private static Expression randomOptionalChild() {
        return randomBoolean() ? null : randomChild();
    }

    public void testHistogramBucketCannotBeSerializedToOldNode() throws IOException {
        Count count = new Count(randomSource(), randomChild(), randomChild());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            PlanStreamOutput planOut = new PlanStreamOutput(out, configuration());
            planOut.setTransportVersion(TransportVersionUtils.randomVersionNotSupporting(Count.ESQL_COUNT_HISTOGRAM_BUCKET));
            UnsupportedOperationException exception = expectThrows(
                UnsupportedOperationException.class,
                () -> planOut.writeNamedWriteable(count)
            );
            assertThat(exception.getMessage(), equalTo("version does not support count(histogram, bucket)"));
        }
    }
}
